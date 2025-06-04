// src/services/exchangeRateService.js
const axios = require('axios');
const config = require('../config');
const logger = require('../config/logger');
const { AppError, ExternalServiceError, NotFoundError } = require('../utils/customErrors');

const SERVICE_NAME = 'ExchangeRateSvc';
const CACHE_DURATION_MS = 3 * 60 * 60 * 1000; // 3시간 (밀리초)

// 환율 캐시
let cachedRate = null;
let lastFetchTime = null;

/**
 * OpenExchangeRates API에서 최신 환율 정보를 가져옵니다.
 * @returns {Promise<Object>} 환율 데이터
 */
async function fetchExchangeRatesFromAPI() {
  const appId = process.env.OPENEXCHANGERATES_APP_ID || config.openExchangeRates?.appId;
  const apiUrl = process.env.OPENEXCHANGERATES_API_URL || config.openExchangeRates?.apiUrl || 'https://openexchangerates.org/api';
  
  if (!appId) {
    logger.error(`[${SERVICE_NAME}] OpenExchangeRates API App ID가 설정되지 않았습니다.`);
    throw new AppError('OpenExchangeRates API 인증 정보가 없습니다.', 500, 'EXCHANGE_RATE_CONFIG_ERROR');
  }

  const url = `${apiUrl}/latest.json`;
  
  try {
    logger.info(`[${SERVICE_NAME}] OpenExchangeRates API에서 환율 정보를 가져오는 중...`);
    
    const response = await axios.get(url, {
      params: {
        app_id: appId,
        base: 'USD', // USD 기준 환율
        symbols: 'KRW' // KRW 환율만 가져오기
      },
      timeout: 10000 // 10초 타임아웃
    });

    if (response.data && response.data.rates && response.data.rates.KRW) {
      logger.info(`[${SERVICE_NAME}] 환율 정보 가져오기 성공. USD to KRW: ${response.data.rates.KRW}`);
      return response.data;
    } else {
      throw new Error('OpenExchangeRates API 응답 형식이 올바르지 않습니다.');
    }
  } catch (error) {
    if (error.response) {
      const status = error.response.status;
      const message = error.response.data?.message || error.message;
      logger.error(`[${SERVICE_NAME}] OpenExchangeRates API 오류: ${status} - ${message}`);
      
      if (status === 401) {
        throw new ExternalServiceError(SERVICE_NAME, error, 'OpenExchangeRates API 인증 실패', 'EXCHANGE_RATE_AUTH_ERROR');
      } else if (status === 429) {
        throw new ExternalServiceError(SERVICE_NAME, error, 'OpenExchangeRates API 요청 한도 초과', 'EXCHANGE_RATE_LIMIT_ERROR');
      }
    }
    
    logger.error(`[${SERVICE_NAME}] 환율 정보 가져오기 실패: ${error.message}`, { stack: error.stack });
    throw new ExternalServiceError(SERVICE_NAME, error, 'OpenExchangeRates에서 환율 정보를 가져올 수 없습니다.');
  }
}

/**
 * KRW에서 USD로 변환하는 환율을 가져옵니다. (1 KRW = ? USD)
 * 캐시된 값이 있고 유효하면 사용하고, 그렇지 않으면 새로 가져옵니다.
 * @returns {Promise<number>} 1 KRW당 USD 환율
 */
async function getKrwToUsdRate() {
  const now = Date.now();
  
  // 유효한 캐시된 환율이 있는지 확인
  if (cachedRate !== null && lastFetchTime !== null) {
    const timeSinceLastFetch = now - lastFetchTime;
    if (timeSinceLastFetch < CACHE_DURATION_MS) {
      logger.debug(`[${SERVICE_NAME}] 캐시된 환율 사용: 1 KRW = ${cachedRate} USD (${Math.round(timeSinceLastFetch / 60000)}분 전 캐시됨)`);
      return cachedRate;
    }
  }
  
  try {
    // 새로운 환율 데이터 가져오기
    const exchangeData = await fetchExchangeRatesFromAPI();
    
    // KRW to USD 환율 계산 (1 KRW = ? USD)
    // API에서는 USD to KRW (1 USD = X KRW)를 제공하므로 역수 계산
    const usdToKrw = exchangeData.rates.KRW;
    const krwToUsd = 1 / usdToKrw;
    
    // 캐시 업데이트
    cachedRate = krwToUsd;
    lastFetchTime = now;
    
    logger.info(`[${SERVICE_NAME}] 환율 캐시 업데이트: 1 KRW = ${krwToUsd.toFixed(8)} USD (1 USD = ${usdToKrw} KRW)`);
    
    return krwToUsd;
  } catch (error) {
    // 새로운 환율 가져오기에 실패했지만 캐시된 값이 있으면 사용
    if (cachedRate !== null) {
      logger.warn(`[${SERVICE_NAME}] 새로운 환율 가져오기 실패, 캐시된 환율 사용: 1 KRW = ${cachedRate} USD`);
      return cachedRate;
    }
    
    // 캐시된 환율도 없으면 에러 발생
    logger.error(`[${SERVICE_NAME}] 사용 가능한 캐시된 환율이 없고 새로운 환율도 가져올 수 없습니다.`);
    throw error;
  }
}

/**
 * 환율 캐시를 강제로 새로고침합니다.
 * @returns {Promise<number>} 새로운 1 KRW당 USD 환율
 */
async function refreshExchangeRate() {
  logger.info(`[${SERVICE_NAME}] 환율 캐시 강제 새로고침 중...`);
  
  // 캐시 삭제하여 강제로 새로 가져오도록 함
  cachedRate = null;
  lastFetchTime = null;
  
  return await getKrwToUsdRate();
}

/**
 * 현재 캐시된 환율 정보를 가져옵니다.
 * @returns {Object|null} 캐시된 환율 정보 또는 null
 */
function getCachedRateInfo() {
  if (cachedRate === null || lastFetchTime === null) {
    return null;
  }
  
  const now = Date.now();
  const age = now - lastFetchTime;
  const expiresIn = Math.max(0, CACHE_DURATION_MS - age);
  
  return {
    rate: cachedRate,
    lastFetchTime: new Date(lastFetchTime),
    ageMinutes: Math.round(age / 60000),
    expiresInMinutes: Math.round(expiresIn / 60000),
    isExpired: age >= CACHE_DURATION_MS
  };
}

// 3시간마다 자동 새로고침을 위한 인터벌
let refreshInterval = null;

/**
 * 3시간마다 환율을 자동으로 새로고침하도록 설정합니다.
 */
function startAutoRefresh() {
  if (refreshInterval) {
    logger.warn(`[${SERVICE_NAME}] 환율 자동 새로고침이 이미 실행 중입니다.`);
    return;
  }
  
  // 초기 환율 가져오기
  getKrwToUsdRate().catch(err => {
    logger.error(`[${SERVICE_NAME}] 초기 환율 가져오기 실패:`, err);
  });
  
  // 3시간마다 자동 새로고침 설정
  refreshInterval = setInterval(() => {
    refreshExchangeRate().catch(err => {
      logger.error(`[${SERVICE_NAME}] 자동 환율 새로고침 실패:`, err);
    });
  }, CACHE_DURATION_MS);
  
  logger.info(`[${SERVICE_NAME}] 환율 자동 새로고침 시작 (매 ${CACHE_DURATION_MS / 3600000}시간마다)`);
}

/**
 * 환율 자동 새로고침을 중지합니다.
 */
function stopAutoRefresh() {
  if (refreshInterval) {
    clearInterval(refreshInterval);
    refreshInterval = null;
    logger.info(`[${SERVICE_NAME}] 환율 자동 새로고침 중지됨`);
  }
}

// 모듈 로드 시 자동으로 환율 가져오기 시작
// 필요에 따라 주석 해제하여 사용
// startAutoRefresh();

module.exports = {
  getKrwToUsdRate,
  refreshExchangeRate,
  getCachedRateInfo,
  startAutoRefresh,
  stopAutoRefresh
};