// src/mappers/orderMapper.js
// Shopify 주문 데이터를 번개장터 주문 생성 API가 요구하는 형식으로 변환합니다.
// 번개장터 "Create Order V2" API 명세를 기준으로 합니다.

const logger = require('../config/logger');
const { AppError, ValidationError } = require('../utils/customErrors');

/**
 * Shopify 주문의 특정 line item과 해당 번개장터 상품 상세 정보를 바탕으로
 * 번개장터 "Create Order V2" API 페이로드를 생성합니다.
 * @param {object} shopifyLineItem - 주문 처리 대상 Shopify line_item 객체.
 * @param {string} bunjangPid - 해당 상품의 번개장터 PID (숫자형 문자열).
 * @param {object} bunjangProductDetails - bunjangService.getBunjangProductDetails로 조회한 번개장터 상품 상세 정보.
 * (price, shippingFee 등 KRW 기준 정보 포함)
 * @returns {object} 번개장터 Create Order V2 API 페이로드.
 * @throws {ValidationError|AppError} 필수 데이터 누락 또는 유효하지 않은 경우.
 */
function mapShopifyItemToBunjangOrderPayload(shopifyLineItem, bunjangPid, bunjangProductDetails) {
  // 입력값 기본 검증
  if (!shopifyLineItem || !bunjangPid || !bunjangProductDetails) {
    throw new ValidationError('번개장터 주문 페이로드 매핑을 위한 필수 데이터가 누락되었습니다.', [
        { field: 'shopifyLineItem', message: !shopifyLineItem ? 'Shopify line item 누락' : undefined },
        { field: 'bunjangPid', message: !bunjangPid ? '번개장터 PID 누락' : undefined },
        { field: 'bunjangProductDetails', message: !bunjangProductDetails ? '번개장터 상품 상세 정보 누락' : undefined },
    ].filter(e => e.message));
  }

  const { price: bunjangKrwPrice, shippingFee: bunjangKrwShippingFee } = bunjangProductDetails;

  if (typeof bunjangKrwPrice === 'undefined' || typeof bunjangKrwShippingFee === 'undefined') {
    throw new AppError(`번개장터 상품(PID: ${bunjangPid}) 상세 정보에 가격 또는 배송비가 없습니다.`, 500, 'BUNJANG_PRODUCT_DATA_INCOMPLETE', true, { bunjangPid });
  }

  const currentBunjangPriceKrw = parseInt(bunjangKrwPrice, 10);
  const currentBunjangShippingFeeKrw = parseInt(bunjangKrwShippingFee, 10);

  if (isNaN(currentBunjangPriceKrw) || currentBunjangPriceKrw < 0) {
    throw new ValidationError(`번개장터 상품(PID: ${bunjangPid})의 가격이 유효하지 않습니다: ${bunjangKrwPrice}`, [{ field: 'bunjangProductDetails.price', message: '유효하지 않은 가격' }]);
  }
  if (isNaN(currentBunjangShippingFeeKrw) || currentBunjangShippingFeeKrw < 0) {
    throw new ValidationError(`번개장터 상품(PID: ${bunjangPid})의 배송비가 유효하지 않습니다: ${bunjangKrwShippingFee}`, [{ field: 'bunjangProductDetails.shippingFee', message: '유효하지 않은 배송비' }]);
  }

  // 번개장터 "Create Order V2" API 페이로드:
  // { product: { id: integer, price: integer }, deliveryPrice: integer }
  const payload = {
    product: {
      id: parseInt(bunjangPid, 10),   // 번개장터 상품 ID (숫자)
      price: currentBunjangPriceKrw, // 주문 시점의 실제 번개장터 상품 가격 (KRW, 정수)
    },
    // 요구사항: "주문 시 배송비는 자동으로 0원으로 설정되며, 배송비는 별도로 이메일을 통해 고객에게 청구됨"
    // 위 정책에 따라 deliveryPrice를 0으로 설정.
    // 실제 배송비(currentBunjangShippingFeeKrw)는 orderService에서 별도로 메타필드에 기록.
    deliveryPrice: 0,
  };

  // TODO: 만약 번개장터 주문 API가 구매자 정보, 배송지 정보 등을 받는다면,
  // shopifyOrder 객체(이 함수에는 lineItem만 전달됨. 필요시 전체 shopifyOrder 객체 전달)에서
  // 해당 정보를 추출하여 페이로드에 추가해야 합니다.
  // 예: payload.customer = { name: shopifyOrder.customer.first_name, phone: shopifyOrder.shipping_address.phone };
  //     payload.shippingAddress = { street: shopifyOrder.shipping_address.address1, ... };
  // 현재 "Create Order V2" API 명세에는 이 부분이 없습니다.

  logger.debug(`[OrderMapper] Mapped Bunjang order payload for PID ${bunjangPid}:`, payload);
  return payload;
}

module.exports = {
  mapShopifyItemToBunjangOrderPayload,
};
