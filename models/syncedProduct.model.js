// src/models/syncedProduct.model.js
const mongoose = require('mongoose');

const syncedProductSchema = new mongoose.Schema({
  bunjangPid: {
    type: String, required: true, unique: true, index: true, trim: true,
  },
  shopifyGid: { // 예: "gid://shopify/Product/1234567890"
    type: String, unique: true, sparse: true, index: true, trim: true,
  },
  shopifyProductId: { // 예: "1234567890" (숫자 ID)
    type: String, index: true, sparse: true, trim: true,
  },
  shopifyHandle: {
    type: String, index: true, sparse: true, trim: true,
  },
  // 번개장터 원본 정보 (참고 및 동기화 비교용)
  bunjangProductName: { type: String, trim: true },
  bunjangCategoryId: { type: String, index: true, trim: true },
  bunjangBrandId: { type: String, index: true, trim: true },
  bunjangSellerUid: { type: String, index: true, trim: true },
  bunjangCondition: { type: String, trim: true },
  bunjangOriginalPriceKrw: { type: Number },
  bunjangOriginalShippingFeeKrw: { type: Number },
  bunjangQuantity: { type: Number }, // 카탈로그 기준 재고
  bunjangOptionsJson: { type: String }, // 번개장터 옵션 원본 JSON 문자열
  bunjangImagesJson: { type: String }, // 번개장터 이미지 URL 목록 원본 JSON 문자열
  bunjangKeywordsJson: { type: String }, // 번개장터 키워드 목록 원본 JSON 문자열
  bunjangCreatedAt: { type: Date }, // 번개장터 상품 생성 시간 (KST)
  bunjangUpdatedAt: { type: Date, index: true }, // 번개장터 상품 수정 시간 (KST, 카탈로그 기준)

  // Shopify 연동 정보
  shopifyProductType: { type: String, index: true, trim: true }, // 매핑된 Shopify 상품 유형
  shopifyListedPriceUsd: { type: String }, // Shopify에 리스팅된 USD 가격 문자열 (예: "25.99")
  shopifyStatus: { type: String, enum: ['ACTIVE', 'DRAFT', 'ARCHIVED'], index: true }, // Shopify 상품 상태

  // 동기화 상태 및 이력
  lastSyncAttemptAt: { type: Date, default: Date.now, index: true },
  lastSuccessfulSyncAt: { type: Date, index: true },
  syncStatus: {
    type: String,
    enum: ['SYNCED', 'ERROR', 'PENDING', 'PARTIAL_ERROR', 'SKIPPED_NO_CHANGE'],
    default: 'PENDING',
    index: true,
  },
  syncErrorMessage: { type: String, maxlength: 1000 },
  syncErrorStackSample: { type: String, maxlength: 2000 },
  syncRetryCount: { type: Number, default: 0, index: true },
  
  // 추가적인 내부 관리 필드
  isFilteredOut: { type: Boolean, default: false, index: true }, // 카테고리 등으로 필터링 아웃된 상품 표시
  notes: { type: String, maxlength: 500 }, // 관리자 메모

}, {
  timestamps: true, // createdAt, updatedAt (Mongoose 문서 자체의 생성/수정 시간)
  versionKey: false,
  minimize: false, // 빈 객체도 저장 (bunjangOptions 등)
});

// 복합 인덱스 (필요에 따라 추가)
// 예: syncedProductSchema.index({ syncStatus: 1, lastSyncAttemptAt: -1 }); // 특정 상태의 오래된 시도 찾기
// 예: syncedProductSchema.index({ shopifyProductType: 1, shopifyListedPriceUsd: 1 }); // App Proxy 검색용

// 텍스트 인덱스 (App Proxy 검색용 - bunjangProductName, shopifyTitle 등)
// syncedProductSchema.index({ bunjangProductName: 'text', shopifyTitle: 'text', bunjangKeywordsJson: 'text' });
// 텍스트 인덱스는 컬렉션당 하나만 가능. 필요한 필드를 신중하게 선택.

// TTL 인덱스 (선택 사항: 특정 조건의 문서를 자동으로 삭제)
// 예: 30일 동안 업데이트 안 된 'ERROR' 상태의 문서를 자동 삭제
// syncedProductSchema.index({ lastSyncAttemptAt: 1 }, { expireAfterSeconds: 30 * 24 * 60 * 60, partialFilterExpression: { syncStatus: 'ERROR' } });


// 중복 방지를 위해 bunjangPid는 반드시 unique해야 함 (스키마에서 이미 unique: true 설정)

const SyncedProduct = mongoose.model('SyncedProduct', syncedProductSchema);

module.exports = SyncedProduct;
