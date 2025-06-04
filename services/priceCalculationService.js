// src/services/priceCalculationService.js
// 번개장터 상품의 원화 가격을 기준으로 Shopify 판매 가격(USD)을 계산합니다.

const config = require('../config');
const logger = require('../config/logger');
const { getKrwToUsdRate } = require('./exchangeRateService');
const { AppError, ValidationError } = require('../utils/customErrors');

/**
 * KRW 금액을 USD로 변환합니다.
 * @param {number} krwAmount - 원화 금액.
 * @param {number} krwToUsdRate - 1 KRW당 USD 환율.
 * @returns {number} 변환된 USD 금액.
 * @throws {ValidationError} 입력값이 유효하지 않은 경우.
 */
function convertKrwToUsd(krwAmount, krwToUsdRate) {
  if (typeof krwAmount !== 'number' || isNaN(krwAmount) || krwAmount < 0) {
    throw new ValidationError('KRW 금액은 0 이상의 숫자여야 합니다.', [{ field: 'krwAmount', message: '유효하지 않은 KRW 금액입니다.' }]);
  }
  if (typeof krwToUsdRate !== 'number' || isNaN(krwToUsdRate) || krwToUsdRate <= 0) {
    // 이 에러는 getKrwToUsdRate에서 이미 처리될 가능성이 높음
    throw new AppError('KRW-USD 환율이 유효하지 않습니다.', 500, 'INVALID_EXCHANGE_RATE_INTERNAL');
  }
  return krwAmount * krwToUsdRate;
}

/**
 * 최종 Shopify 리스팅 가격(USD)을 계산합니다.
 * 로직: (원화 가격 * 환율) * (1 + 마크업 비율/100) + 취급 수수료
 * 가격은 소수점 둘째 자리까지 반올림된 문자열로 반환됩니다.
 * @param {number} bunjangPriceKrw - 번개장터 상품의 원화 가격.
 * @returns {Promise<string>} 계산된 최종 USD 가격 (문자열, 예: "27.88").
 * @throws {AppError|ValidationError} 환율 정보를 가져오지 못하거나 계산 중 문제 발생 시.
 */
async function calculateShopifyPriceUsd(bunjangPriceKrw) {
  logger.debug(`[PriceCalcSvc] Calculating Shopify price for Bunjang KRW price: ${bunjangPriceKrw}`);

  if (typeof bunjangPriceKrw !== 'number' || isNaN(bunjangPriceKrw) || bunjangPriceKrw < 0) {
    throw new ValidationError('번개장터 상품 가격(KRW)은 0 이상의 숫자여야 합니다.', [{ field: 'bunjangPriceKrw', message: '유효하지 않은 번개장터 원화 가격입니다.' }]);
  }

  let krwToUsdRate;
  try {
    krwToUsdRate = await getKrwToUsdRate(); // 내부적으로 NotFoundError 등 AppError 발생 가능
  } catch (rateError) {
    logger.error(`[PriceCalcSvc] Failed to get exchange rate for price calculation: ${rateError.message}`);
    // 환율 서비스 에러를 그대로 전달하거나, 더 사용자 친화적인 메시지로 변경
    throw new AppError(`가격 계산에 필요한 환율 정보를 가져올 수 없습니다. (원인: ${rateError.message})`, 503, 'EXCHANGE_RATE_UNAVAILABLE_FOR_PRICE_CALC', true, rateError);
  }

  logger.debug(`[PriceCalcSvc] Using KRW to USD rate: ${krwToUsdRate}`);

  // 1. 원화 가격을 USD로 변환
  const priceInUsdBeforeMarkup = convertKrwToUsd(bunjangPriceKrw, krwToUsdRate);

  // 2. 마크업 적용 (config에서 % 단위로 가져옴)
  const markupRatio = config.priceCalculation.markupPercentage / 100;
  const priceAfterMarkup = priceInUsdBeforeMarkup * (1 + markupRatio);

  // 3. 취급 수수료 추가
  const handlingFeeUsd = config.priceCalculation.handlingFeeUsd;
  const finalPriceUsd = priceAfterMarkup + handlingFeeUsd;

  // Shopify 가격은 보통 문자열로, 소수점 2자리까지 (반올림)
  // 정확한 반올림을 위해 (예: 금융 계산 시) BigNumber 라이브러리 사용 고려 가능
  // 여기서는 Number.prototype.toFixed() 사용 (일반적인 경우 충분)
  const shopifyPriceString = finalPriceUsd.toFixed(2);

  logger.info(`[PriceCalcSvc] Calculated Shopify price for KRW ${bunjangPriceKrw} -> USD ${shopifyPriceString}. (Rate: ${krwToUsdRate.toFixed(6)}, BeforeMarkup: ${priceInUsdBeforeMarkup.toFixed(4)}, AfterMarkup: ${priceAfterMarkup.toFixed(4)}, Final: ${finalPriceUsd.toFixed(4)})`);

  return shopifyPriceString;
}

/**
 * (참고용) 번개장터 주문 시 내부적으로 사용될 수 있는 총 예상 비용(USD)을 계산합니다.
 * 이 가격은 Shopify에 직접 리스팅되는 가격이 아니며, 별도 청구될 배송비를 포함할 수 있습니다.
 * @param {number} bunjangPriceKrw - 번개장터 상품의 원화 가격.
 * @param {number} bunjangShippingFeeKrw - 번개장터 상품의 원화 배송비.
 * @returns {Promise<object|null>} 계산된 비용 상세 객체 또는 실패 시 null.
 * { itemPriceKrw, shippingFeeKrw, exchangeRate, itemPriceUsd, shippingFeeUsd, handlingFeeUsd, totalEstimatedCostUsd }
 */
async function calculateInternalTotalCostUsd(bunjangPriceKrw, bunjangShippingFeeKrw) {
    if (typeof bunjangPriceKrw !== 'number' || isNaN(bunjangPriceKrw) || bunjangPriceKrw < 0 ||
        typeof bunjangShippingFeeKrw !== 'number' || isNaN(bunjangShippingFeeKrw) || bunjangShippingFeeKrw < 0) {
        logger.warn('[PriceCalcSvc] Invalid input for internal cost calculation.', { bunjangPriceKrw, bunjangShippingFeeKrw });
        return null; // 또는 ValidationError throw
    }

    let krwToUsdRate;
    try {
        krwToUsdRate = await getKrwToUsdRate();
    } catch (rateError) {
        logger.error(`[PriceCalcSvc] Failed to get exchange rate for internal cost calculation: ${rateError.message}`);
        return null; // 또는 AppError throw
    }

    const priceInUsdBeforeMarkup = convertKrwToUsd(bunjangPriceKrw, krwToUsdRate);
    const markupRatio = config.priceCalculation.markupPercentage / 100;
    const itemPriceUsd = priceInUsdBeforeMarkup * (1 + markupRatio); // 마크업된 품목 가격 (USD)
    
    const shippingFeeUsd = convertKrwToUsd(bunjangShippingFeeKrw, krwToUsdRate); // 배송비 (USD)
    const handlingFeeUsd = config.priceCalculation.handlingFeeUsd; // 취급 수수료 (USD)

    // 총 예상 비용 = (마크업된 품목가 USD) + (배송비 USD) + (취급수수료 USD)
    const totalEstimatedCostUsd = itemPriceUsd + shippingFeeUsd + handlingFeeUsd;

    const result = {
        itemPriceKrw: bunjangPriceKrw,
        shippingFeeKrw: bunjangShippingFeeKrw,
        exchangeRateUsed: parseFloat(krwToUsdRate.toFixed(8)), // 사용된 환율 (소수점 많이)
        itemPriceUsd: parseFloat(itemPriceUsd.toFixed(2)),
        shippingFeeUsd: parseFloat(shippingFeeUsd.toFixed(2)),
        handlingFeeUsd: parseFloat(handlingFeeUsd.toFixed(2)),
        totalEstimatedCostUsd: parseFloat(totalEstimatedCostUsd.toFixed(2)),
    };
    logger.debug('[PriceCalcSvc] Calculated internal total cost (USD):', result);
    return result;
}


module.exports = {
  calculateShopifyPriceUsd,
  calculateInternalTotalCostUsd, // 필요시 사용
  // convertKrwToUsd, // 내부 사용으로 변경
};
