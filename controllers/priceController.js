// src/controllers/priceController.js
// 가격 계산 로직 테스트용 API 엔드포인트 핸들러. (내부 관리용 또는 개발용)

const logger = require('../config/logger');
const { calculateShopifyPriceUsd, calculateInternalTotalCostUsd } = require('../services/priceCalculationService');
const { validationResult } = require('express-validator'); // express-validator 결과 처리
const { ValidationError, AppError } = require('../utils/customErrors'); // 커스텀 에러

/**
 * GET /api/price/calculate-shopify?krwPrice=10000[&krwShippingFee=3000]
 * 주어진 KRW 가격에 대해 Shopify 리스팅 가격(USD) 및 내부 비용을 계산하여 반환합니다.
 */
async function getCalculatedShopifyPrice(req, res, next) {
  // express-validator를 사용한 경우, 에러는 handleValidationErrors 미들웨어에서 처리됨.
  // 여기서는 수동으로 validationResult를 확인하거나, 라우트에서 handleValidationErrors를 사용.
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    // ValidationError를 throw하여 중앙 에러 핸들러가 처리하도록 함
    throw new ValidationError('입력값 유효성 검사 실패.', errors.array());
  }

  const krwPrice = parseFloat(req.query.krwPrice); // express-validator toFloat() 사용 시 이미 숫자
  const krwShippingFee = req.query.krwShippingFee ? parseFloat(req.query.krwShippingFee) : 0;


  logger.info(`[PriceCtrlr] API call to calculate Shopify price for KRW: ${krwPrice}, ShippingKRW: ${krwShippingFee}`);
  try {
    const shopifyPriceUsd = await calculateShopifyPriceUsd(krwPrice);
    const internalCostDetails = await calculateInternalTotalCostUsd(krwPrice, krwShippingFee);

    res.status(200).json({
      inputs: {
        bunjangPriceKrw: krwPrice,
        bunjangShippingFeeKrw: krwShippingFee,
      },
      calculatedShopifyListingPriceUsd: shopifyPriceUsd,
      estimatedInternalCostsUsd: internalCostDetails,
    });
  } catch (error) {
    // priceCalculationService에서 AppError, ValidationError, ExternalServiceError 등을 throw 할 수 있음
    logger.error(`[PriceCtrlr] Error calculating Shopify price for KRW ${krwPrice}: ${error.message}`, {
        errorCode: error.errorCode, details: error.details, stack: error.stack?.substring(0,300)
    });
    // 에러를 중앙 에러 핸들러로 전달
    next(error);
  }
}

module.exports = {
  getCalculatedShopifyPrice,
};
