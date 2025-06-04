// src/services/orderService.js
// Shopify 주문 웹훅 수신 후 번개장터 주문 생성 등의 로직을 담당합니다.
// 이 서비스의 주요 함수(processShopifyOrderForBunjang)는 BullMQ 주문 워커에 의해 실행됩니다.

const config = require('../config');
const logger =require('../config/logger');
const bunjangService = require('./bunjangService');
const shopifyService = require('./shopifyService');
const orderMapper = require('../mappers/orderMapper');
const { AppError, ExternalServiceError, NotFoundError, ValidationError } = require('../utils/customErrors');
// const SyncedProduct = require('../models/syncedProduct.model'); // 상품 원본가 참조 시 필요

/**
 * Shopify 주문 데이터를 기반으로 번개장터에 주문을 생성합니다.
 * @param {object} shopifyOrder - Shopify 주문 객체 (웹훅 페이로드 또는 DB에서 가져온 객체).
 * @param {string} [jobId='N/A'] - 호출한 BullMQ 작업 ID (로깅용).
 * @returns {Promise<{success: boolean, bunjangOrderId?: string, message?: string}>} 처리 결과.
 */
async function processShopifyOrderForBunjang(shopifyOrder, jobId = 'N/A') {
  const shopifyOrderId = shopifyOrder.id; // Shopify REST API ID
  const shopifyOrderGid = shopifyOrder.admin_graphql_api_id; // Shopify GraphQL GID
  logger.info(`[OrderSvc:Job-${jobId}] Processing Shopify Order ID: ${shopifyOrderId} (GID: ${shopifyOrderGid}) for Bunjang.`);

  // Shopify 주문 객체 유효성 검사
  if (!shopifyOrder || !shopifyOrderId || !shopifyOrderGid || !Array.isArray(shopifyOrder.line_items) || shopifyOrder.line_items.length === 0) {
    throw new ValidationError('유효하지 않은 Shopify 주문 데이터입니다. (ID 또는 line_items 누락)', [{field: 'shopifyOrder', message: 'Order data invalid or missing line items.'}]);
  }

  const bunjangOrderIdentifier = `${config.bunjang.orderIdentifierPrefix}${shopifyOrderId}`;
  let bunjangOrderSuccessfullyCreatedOverall = false;
  let createdBunjangOrderIds = [];

  // TODO: 이미 이 Shopify 주문에 대해 번개장터 주문이 생성되었는지 확인하는 로직 추가
  // 예: Shopify 주문 메타필드 `bunjang.order_id` 조회 또는 내부 DB (ProcessedOrders 모델 등) 조회
  // const existingBunjangOrder = await shopifyService.getOrderMetafield(shopifyOrderGid, "bunjang", "order_id");
  // if (existingBunjangOrder && existingBunjangOrder.value) {
  //   logger.info(`[OrderSvc:Job-${jobId}] Bunjang order already exists (ID: ${existingBunjangOrder.value}) for Shopify Order ${shopifyOrderId}. Skipping.`);
  //   return { success: true, alreadyProcessed: true, bunjangOrderId: existingBunjangOrder.value };
  // }


  // Shopify 주문의 각 line item을 순회 (요구사항: 타 상품과의 구분을 위해 번개장터 주문건에는 'bungjang)' 등의 식별자 포함)
  // -> 이는 Shopify 상품 SKU 또는 태그로 번개장터 연동 상품을 식별하는 것을 의미.
  for (const item of shopifyOrder.line_items) {
    if (!item.sku || !item.sku.startsWith('BJ-')) { // 'BJ-' 프리픽스로 번개장터 연동 상품 SKU 식별
      logger.debug(`[OrderSvc:Job-${jobId}] Shopify item SKU "${item.sku}" (Order: ${shopifyOrderId}) is not a Bunjang-linked product. Skipping this item.`);
      continue;
    }

    const bunjangPid = item.sku.substring(3); // 'BJ-' 제외한 실제 번개장터 상품 ID
    logger.info(`[OrderSvc:Job-${jobId}] Found Bunjang-linked item for Order ${shopifyOrderId}: Shopify SKU ${item.sku} -> Bunjang PID ${bunjangPid}`);

    try {
      // 1. 주문 시점의 번개장터 상품 최신 정보 조회 (가격, 배송비 등 KRW 기준)
      const bunjangProductDetails = await bunjangService.getBunjangProductDetails(bunjangPid);
      if (!bunjangProductDetails) {
        // 상품 조회 실패 시, 이 아이템에 대한 번개장터 주문 생성 불가
        logger.warn(`[OrderSvc:Job-${jobId}] Could not fetch details for Bunjang product PID ${bunjangPid} (Order: ${shopifyOrderId}). Cannot create Bunjang order for this item.`);
        await shopifyService.updateOrder({ id: shopifyOrderGid, tags: [`${bunjangOrderIdentifier}_Error`, `PID-${bunjangPid}-NotFound`] });
        continue; // 다음 아이템으로 (또는 전체 주문 실패 처리)
      }

      // 2. 번개장터 "Create Order V2" API 페이로드 구성 (orderMapper 사용)
      const bunjangOrderPayload = orderMapper.mapShopifyItemToBunjangOrderPayload(item, bunjangPid, bunjangProductDetails);
      if (!bunjangOrderPayload) {
        logger.error(`[OrderSvc:Job-${jobId}] Failed to map Bunjang order payload for PID ${bunjangPid} (Order: ${shopifyOrderId}).`);
        await shopifyService.updateOrder({ id: shopifyOrderGid, tags: [`${bunjangOrderIdentifier}_Error`, `PID-${bunjangPid}-MapFail`] });
        continue;
      }
      
      // 3. 배송비 0원 정책 적용 (요구사항)
      //    "주문 시 배송비는 자동으로 0원으로 설정되며, 배송비는 별도로 이메일을 통해 고객에게 청구됨"
      //    orderMapper에서 생성된 payload의 deliveryPrice를 0으로 덮어쓰고, 실제 배송비는 메타필드에 기록.
      const actualBunjangShippingFeeKrw = bunjangOrderPayload.deliveryPrice; // Mapper가 계산한 실제 배송비
      bunjangOrderPayload.deliveryPrice = 0; // API 요청 시 배송비 0으로 설정
      logger.info(`[OrderSvc:Job-${jobId}] Applying 0 KRW delivery fee policy for PID ${bunjangPid}. Actual Bunjang shipping fee was: ${actualBunjangShippingFeeKrw} KRW.`);


      // 4. 번개장터 주문 생성 API 호출
      logger.info(`[OrderSvc:Job-${jobId}] Attempting to create Bunjang order for PID ${bunjangPid} (Order: ${shopifyOrderId}) with payload:`, bunjangOrderPayload);
      const bunjangApiResponse = await bunjangService.createBunjangOrderV2(bunjangOrderPayload); // response.data.data = { id: newOrderId }

      if (bunjangApiResponse && bunjangApiResponse.id) { // API 응답에서 주문 ID 직접 사용
        const bunjangOrderId = bunjangApiResponse.id;
        logger.info(`[OrderSvc:Job-${jobId}] Successfully created Bunjang order for PID ${bunjangPid} (Order: ${shopifyOrderId}). Bunjang Order ID: ${bunjangOrderId}`);
        createdBunjangOrderIds.push(String(bunjangOrderId));
        bunjangOrderSuccessfullyCreatedOverall = true; // 하나라도 성공하면 전체 성공으로 간주 (정책에 따라 다를 수 있음)

        // 5. Shopify 주문에 태그 및 메타필드 추가
        const tagsToAdd = ['BunjangOrderPlaced', bunjangOrderIdentifier, `BunjangOrderID-${bunjangOrderId}`];
        const metafieldsInput = [
          { namespace: "bunjang", key: "order_id", value: String(bunjangOrderId), type: "single_line_text_field" },
          { namespace: "bunjang", key: "ordered_pid", value: String(bunjangPid), type: "single_line_text_field" },
          { namespace: "bunjang", key: "ordered_item_price_krw", value: String(bunjangOrderPayload.product.price), type: "number_integer" },
          { namespace: "bunjang", key: "api_sent_shipping_fee_krw", value: String(bunjangOrderPayload.deliveryPrice), type: "number_integer" }, // API에 보낸 배송비 (0)
          { namespace: "bunjang", key: "actual_bunjang_shipping_fee_krw", value: String(actualBunjangShippingFeeKrw), type: "number_integer" }, // 실제 배송비 (별도 청구용)
        ];
        // 여러 메타필드와 태그를 한 번의 orderUpdate 호출로 처리하는 것이 효율적
        await shopifyService.updateOrder({ id: shopifyOrderGid, tags: tagsToAdd, metafields: metafieldsInput });
        
        // TODO: "갑(씨에스트레이딩)이 선구매한 번개장터 크레딧을 활용해 자동으로 번개장터 내 해당 상품 주문"
        // 이 부분은 번개장터 API가 크레딧 사용을 지원하는 방식에 따라 추가 구현 필요.
        // 현재 Create Order V2 API에는 크레딧 관련 파라미터가 없음. 별도 API 호출 또는 프로세스 필요.

        // TODO: "배송지는 서울시 금천구 디지털로 130, 남성프라자 908호(수령인: (번장)문장선 또는 (번장)씨에스트레이딩)"
        // Create Order V2 API 명세에 배송지 입력 부분이 없음. 주문 후 별도 API로 배송 정보를 업데이트하거나,
        // 번개장터 파트너 계정에 기본 배송지가 설정되어 사용될 수 있음. 확인 및 추가 구현 필요.

        // 한 Shopify 주문에 여러 번개장터 연동 상품이 있을 경우,
        // 각 상품별로 번개장터 주문을 생성할지, 아니면 대표 상품 하나만 주문할지 정책 필요.
        // 현재 코드는 각 유효한 line_item에 대해 번개장터 주문을 시도함.
        // 만약 하나의 Shopify 주문 당 하나의 번개장터 주문만 생성해야 한다면, 첫 성공 후 break.
        // break; // 정책: 첫 번째 성공한 아이템에 대해서만 번개장터 주문 생성 시
      } else {
        // 번개장터 주문 생성 API는 성공했으나, 응답에서 ID를 못 찾은 경우
        logger.error(`[OrderSvc:Job-${jobId}] Bunjang order creation for PID ${bunjangPid} (Order: ${shopifyOrderId}) API call was successful but response missing order ID. Response:`, bunjangApiResponse);
        await shopifyService.updateOrder({ id: shopifyOrderGid, tags: [`${bunjangOrderIdentifier}_Error`, `PID-${bunjangPid}-CreateRespFail`] });
      }

    } catch (error) { // bunjangService.getBunjangProductDetails 또는 createBunjangOrderV2 에서 발생한 에러
      logger.error(`[OrderSvc:Job-${jobId}] Error processing Bunjang order for Shopify item SKU ${item.sku} (PID ${bunjangPid}, Order: ${shopifyOrderId}): ${error.message}`, {
        errorCode: error.errorCode, details: error.details, stack: error.stack?.substring(0,500)
      });
      await shopifyService.updateOrder({ id: shopifyOrderGid, tags: [`${bunjangOrderIdentifier}_Error`, `PID-${bunjangPid}-Exception`] });
      // 개별 상품 주문 실패 시 다음 상품으로 계속 진행. 전체 주문 실패 여부는 createdBunjangOrderIds 배열로 판단.
    }
  } // end of for loop for line_items

  if (bunjangOrderSuccessfullyCreatedOverall) {
    logger.info(`[OrderSvc:Job-${jobId}] Bunjang order(s) (IDs: ${createdBunjangOrderIds.join(', ')}) successfully processed for Shopify Order ID: ${shopifyOrderId}.`);
    return { success: true, bunjangOrderIds: createdBunjangOrderIds, message: `번개장터 주문(들) 생성 성공: ${createdBunjangOrderIds.join(', ')}` };
  } else {
    logger.warn(`[OrderSvc:Job-${jobId}] No Bunjang order was successfully created for Shopify Order ID: ${shopifyOrderId}.`);
    // 필요시 관리자 알림 등의 추가 조치
    // 이 경우, 작업 자체는 성공했으나 (오류 없이 완료), 실제 주문은 안된 것이므로 success: false 반환
    return { success: false, message: 'Shopify 주문에 포함된 번개장터 연동 상품에 대해 번개장터 주문을 생성하지 못했습니다.' };
  }
}

module.exports = {
  processShopifyOrderForBunjang, // BullMQ 주문 워커가 호출
};
