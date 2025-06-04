// src/mappers/productMapper.js
// 번개장터 카탈로그 상품 데이터를 Shopify ProductInput 스키마로 변환합니다.
// 번개장터 API 문서 및 카탈로그 CSV 필드 구조를 최대한 반영합니다.

const config = require('../config');
const logger = require('../config/logger');
const { AppError } = require('../utils/customErrors');

/**
 * 번개장터 카테고리 ID를 Shopify 상품 유형 문자열로 매핑합니다.
 * @param {string} bunjangCategoryId - 번개장터 카테고리 ID.
 * @returns {string} Shopify 상품 유형 문자열.
 */
function mapBunjangCategoryToShopifyProductType(bunjangCategoryId) {
  if (!bunjangCategoryId) return config.bunjang.defaultShopifyProductType;
  const mapping = config.bunjang.categoryToShopifyType; // .env에서 로드된 매핑 객체
  return mapping[String(bunjangCategoryId).trim()] || config.bunjang.defaultShopifyProductType;
}

/**
 * 번개장터 카탈로그 상품 객체와 계산된 Shopify 가격을 Shopify ProductInput 객체로 변환합니다.
 * @param {object} bunjangProduct - catalogService.processCatalogRow를 통해 처리된 번개장터 상품 객체.
 * CSV 필드: pid, name, description, quantity, price, shippingFee, condition, saleStatus, 
 * keywords (array), images (array), categoryId, brandId, options (array of objects), uid, updatedAt, createdAt
 * @param {string} shopifyPriceString - 계산된 최종 Shopify 리스팅 가격 (USD, 문자열 예: "27.88").
 * @returns {object} Shopify ProductInput 객체.
 * @throws {AppError} 필수 데이터 누락 또는 매핑 중 중요 오류 발생 시.
 */
function mapBunjangToShopifyInput(bunjangProduct, shopifyPriceString) {
  if (!bunjangProduct || typeof bunjangProduct.pid === 'undefined' || !shopifyPriceString) {
    throw new AppError('상품 매핑을 위한 필수 데이터(번개장터 상품 또는 Shopify 가격)가 누락되었습니다.', 500, 'PRODUCT_MAPPING_MISSING_DATA');
  }

  try {
    const {
      pid, name, description, quantity, price: bunjangPriceKrw, shippingFee: bunjangShippingFeeKrw,
      condition, keywords, images, categoryId, brandId, options: bunjangOptions, uid: sellerUid,
      updatedAt: bunjangUpdatedAt, createdAt: bunjangCreatedAt, saleStatus
    } = bunjangProduct; // catalogService.processCatalogRow에서 이미 기본 처리됨

    const productType = mapBunjangCategoryToShopifyProductType(categoryId);

    // HTML 정제: description 필드는 HTML일 수 있으므로 정제 필요.
    // 실제 운영에서는 XSS 방지를 위해 DOMPurify 같은 신뢰할 수 있는 HTML Sanitizer 사용 필수!
    // 여기서는 간단히 줄바꿈만 처리하고, 위험할 수 있는 태그 제거 시도.
    let bodyHtml = description ? description.replace(/\n/g, '<br />') : '<p>상품 설명이 제공되지 않았습니다.</p>';
    bodyHtml = bodyHtml.replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, ''); // 기본적인 script 태그 제거

    const sku = `BJ-${pid}`; // Shopify 스토어 내 고유 SKU

    // 이미지: ProductImageInput[] 타입 ({ src, altText })
    const imagesInput = images && images.length > 0
      ? images.slice(0, 250).map((imgUrl, index) => ({ // Shopify 이미지 수 제한 (250개)
          src: imgUrl,
          altText: `${name.substring(0, 50)} image ${index + 1}`.substring(0, 512) // altText 길이 제한
        }))
      : []; // 이미지가 없으면 빈 배열 (Shopify에서 오류 발생하지 않도록)

    // 태그: 번개장터 키워드 + 고정 태그 + 카테고리 + 브랜드 + PID (검색용)
    // 태그는 최대 255자, 상품당 최대 250개
    const tags = [
      ...(keywords || []),
      'BunjangLinked',
      productType, // 예: "K-Pop Goods"
      `bunjang_pid:${pid}`, // 검색 가능한 형태로 PID 저장
      brandId ? `bunjang_brand:${brandId}` : undefined,
      // condition ? `condition:${condition}` : undefined, // 상태도 태그로 (선택적)
    ].map(tag => String(tag || '').trim().substring(0, 255)) // 문자열 변환, 공백제거, 길이제한
     .filter((tag, index, self) => tag && self.indexOf(tag) === index) // 유효하고 중복 없는 태그만
     .slice(0, 250); // 태그 개수 제한

    // 메타필드: 번개장터의 추가 정보 저장 (ProductInput의 metafields는 MetafieldInput[])
    // MetafieldInput: { namespace: String!, key: String!, value: String!, type: String! }
    const metafields = [
      { namespace: "bunjang", key: "pid", value: String(pid), type: "single_line_text_field" },
      { namespace: "bunjang", key: "original_price_krw", value: String(Math.round(bunjangPriceKrw)), type: "number_integer" },
      { namespace: "bunjang", key: "original_shipping_fee_krw", value: String(Math.round(bunjangShippingFeeKrw)), type: "number_integer" },
      { namespace: "bunjang", key: "condition", value: String(condition || 'USED'), type: "single_line_text_field" },
      { namespace: "bunjang", key: "seller_uid", value: String(sellerUid || ''), type: "single_line_text_field" },
      { namespace: "bunjang", key: "category_id", value: String(categoryId || ''), type: "single_line_text_field" },
      { namespace: "bunjang", key: "brand_id", value: String(brandId || ''), type: "single_line_text_field" },
      bunjangCreatedAt ? { namespace: "bunjang", key: "created_at_kst", value: bunjangCreatedAt.toISOString(), type: "date_time" } : null,
      bunjangUpdatedAt ? { namespace: "bunjang", key: "updated_at_kst", value: bunjangUpdatedAt.toISOString(), type: "date_time" } : null,
      (bunjangOptions && bunjangOptions.length > 0) ? { namespace: "bunjang", key: "options_json", value: JSON.stringify(bunjangOptions), type: "json_string" } : null,
    ].filter(Boolean); // null인 메타필드 제거

    // 상품 옵션 및 변형(Variants) 처리
    // 번개장터 'options' 필드: "[{ \"id\": \"색상\", \"value\": \"빨강\" }, { \"id\": \"사이즈\", \"value\": \"M\" }]"
    // Shopify ProductInput:
    //   options: ["색상", "사이즈"] (옵션 이름 배열)
    //   variants: [{ option1: "빨강", option2: "M", price: "...", sku: "...", ... }, ...]
    // 이 부분은 매우 복잡하며, 번개장터 옵션 구조에 따라 동적으로 Shopify variants를 생성해야 함.
    // 현재 구현은 단일 variant를 기본으로 하고, 옵션 정보는 메타필드에 JSON으로 저장.
    // TODO: 실제 다중 옵션 상품 처리 로직 구현 필요.
    const productVariantsInput = [
      {
        price: shopifyPriceString, // 계산된 USD 가격
        sku: sku,
        // inventoryPolicy: 재고 0일 때 판매 계속 여부 ('DENY' 또는 'CONTINUE')
        // 카탈로그 quantity가 0이면 CONTINUE (품절이지만 계속 표시), 0보다 크면 DENY (재고 없으면 판매 중지)
        inventoryPolicy: quantity > 0 ? 'DENY' : 'CONTINUE',
        inventoryQuantities: [
          {
            availableQuantity: quantity > 0 ? quantity : 0,
            locationId: config.shopify.defaultLocationId, // .env 설정된 기본 Location GID
          },
        ],
        // 단일 variant의 경우 Shopify가 자동으로 "Default Title" 옵션을 생성하거나,
        // option1: "Default" 등을 설정할 수 있음. 여기서는 비워둠.
      },
    ];
    
    // 상품 상태: 번개장터 saleStatus가 'SELLING'이면 'ACTIVE', 아니면 'DRAFT' 또는 'ARCHIVED'
    // 카탈로그에는 SELLING만 온다고 가정했으므로 ACTIVE.
    const productStatus = saleStatus === 'SELLING' ? 'ACTIVE' : 'DRAFT';

    const productInput = {
      title: String(name).substring(0, 255), // Shopify 제목 길이 제한
      bodyHtml: bodyHtml,
      vendor: brandId ? `Bunjang (Brand: ${brandId})` : `Bunjang (Seller: ${sellerUid})`, // 공급업체
      productType: productType,
      tags: tags,
      status: productStatus, // 상품 상태 (ACTIVE, DRAFT, ARCHIVED)
      
      images: imagesInput.length > 0 ? imagesInput : undefined, // 이미지가 없으면 필드 제외 가능
      
      metafields: metafields.length > 0 ? metafields : undefined, // 메타필드 없으면 제외

      variants: productVariantsInput,
      // options: ["옵션1 이름", "옵션2 이름"] // 다중 옵션 시 여기에 옵션 이름 배열 설정
    };

    return productInput;

  } catch (error) {
    logger.error(`[ProductMapper] Error mapping Bunjang product PID ${bunjangProduct?.pid} to Shopify input:`, {
      message: error.message, stack: error.stack,
      // bunjangProductData: JSON.stringify(bunjangProduct).substring(0, 500) // 너무 길 수 있음
    });
    // AppError로 래핑하여 일관된 에러 처리
    if (error instanceof AppError) throw error;
    throw new AppError(`상품 데이터 매핑 중 오류 발생 (PID: ${bunjangProduct?.pid}): ${error.message}`, 500, 'PRODUCT_MAPPING_ERROR_UNEXPECTED');
  }
}

module.exports = {
  mapBunjangToShopifyInput,
  // mapBunjangCategoryToShopifyProductType, // 내부 사용으로 변경
};
