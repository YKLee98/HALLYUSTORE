// src/services/bunjangService.js
// 번개장터 API와의 통신을 담당합니다. (인증, API 호출, 기본 에러 처리)

const axios = require('axios');
const config = require('../config');
const logger = require('../config/logger');
const { generateBunjangToken } = require('../utils/jwtHelper');
const { ExternalServiceError, AppError, NotFoundError } = require('../utils/customErrors');
const zlib = require('zlib'); // 카탈로그 압축 해제용 (fileDownloader.js로 옮겨도 됨)

const SERVICE_NAME = 'BunjangAPI';

// Axios 인스턴스 생성 (번개장터 일반 API용)
const bunjangApiClient = axios.create({
  baseURL: config.bunjang.generalApiUrl,
  timeout: config.bunjang.apiTimeoutMs,
  headers: { 'Content-Type': 'application/json' },
});

// Axios 인스턴스 생성 (번개장터 카탈로그 API용)
const bunjangCatalogApiClient = axios.create({
  baseURL: config.bunjang.catalogApiUrl,
  timeout: config.bunjang.catalogDownloadTimeoutMs, // 카탈로그 다운로드는 타임아웃 길게
});

// 요청 인터셉터: 모든 요청에 JWT 토큰 자동 추가
const addAuthTokenInterceptor = (axiosInstance, isCatalogApi = false) => {
  axiosInstance.interceptors.request.use(
    (axiosReqConfig) => {
      try {
        // GET 요청에도 nonce 포함 (번개장터 Node.js 샘플 코드 기준)
        // 실제 API 테스트 후 GET 요청 시 nonce가 불필요하면 generateBunjangToken(false)로 변경.
        // 카탈로그 API는 GET이지만, 다른 일반 API는 POST/PUT/DELETE일 수 있으므로 includeNonce=true 기본 사용.
        const token = generateBunjangToken(true); 
        axiosReqConfig.headers['Authorization'] = `Bearer ${token}`;
        // logger.debug(`[BunjangSvc] Added JWT to ${isCatalogApi ? 'catalog' : 'general'} API request. URL: ${axiosReqConfig.url}`);
      } catch (jwtError) {
        // JWT 생성 실패 시 요청을 보내지 않고 바로 에러 throw
        logger.error(`[BunjangSvc] Failed to generate JWT for ${isCatalogApi ? 'catalog' : 'general'} API request: ${jwtError.message}`);
        return Promise.reject(new AppError('번개장터 API 인증 토큰 생성 실패.', 500, 'BUNJANG_JWT_ERROR_PRE_REQUEST', true, jwtError));
      }
      return axiosReqConfig;
    },
    (error) => {
      // 요청 설정 중 에러 (거의 발생 안함)
      logger.error(`[BunjangSvc] Error in Bunjang API request interceptor (before send):`, error);
      return Promise.reject(new ExternalServiceError(SERVICE_NAME, error, '번개장터 API 요청 설정 오류'));
    }
  );
};

addAuthTokenInterceptor(bunjangApiClient, false);
addAuthTokenInterceptor(bunjangCatalogApiClient, true);


// 응답 인터셉터: 공통 에러 처리
const handleApiResponseErrorInterceptor = (axiosInstance) => {
  axiosInstance.interceptors.response.use(
    response => response, // 성공 응답은 그대로 통과
    (error) => {
      const requestUrl = error.config?.url;
      const requestMethod = error.config?.method?.toUpperCase();
      if (axios.isAxiosError(error)) {
        const status = error.response?.status;
        const responseData = error.response?.data;
        logger.warn(`[BunjangSvc] Axios error from Bunjang API: ${requestMethod} ${requestUrl}`, {
          status, message: error.message, code: error.code,
          responseData: responseData ? JSON.stringify(responseData).substring(0,500) : undefined, // 너무 길 수 있으므로 요약
        });
        // 번개장터 API 에러 코드에 따른 커스텀 에러 처리
        if (status === 401 || status === 403) { // 인증/권한 오류
          throw new ExternalServiceError(SERVICE_NAME, error, `번개장터 API 인증/권한 오류 (Status: ${status})`, responseData?.errorCode || 'BUNJANG_AUTH_ERROR');
        } else if (status === 404) { // 리소스 없음
          // Product Lookup 등에서 404는 정상적인 "없음"일 수 있으므로, 호출하는 쪽에서 처리하도록 함.
          // 여기서는 일반적인 ExternalServiceError로 throw.
          throw new ExternalServiceError(SERVICE_NAME, error, `번개장터 API 리소스 없음 (Status: 404, URL: ${requestUrl})`, responseData?.errorCode || 'BUNJANG_NOT_FOUND');
        } else if (status >= 400 && status < 500) { // 기타 클라이언트 오류
          throw new ExternalServiceError(SERVICE_NAME, error, `번개장터 API 클라이언트 오류 (Status: ${status})`, responseData?.errorCode || 'BUNJANG_CLIENT_ERROR');
        } else if (status >= 500) { // 서버 오류
          throw new ExternalServiceError(SERVICE_NAME, error, `번개장터 API 서버 오류 (Status: ${status})`, responseData?.errorCode || 'BUNJANG_SERVER_ERROR');
        }
        // 그 외 Axios 에러 (타임아웃, 네트워크 등)
        throw new ExternalServiceError(SERVICE_NAME, error, `번개장터 API 통신 오류 (URL: ${requestUrl})`);
      }
      // Axios 에러가 아닌 경우 (예: 요청 인터셉터에서 발생한 AppError 등)
      logger.error(`[BunjangSvc] Non-Axios error during Bunjang API call to ${requestUrl}:`, error);
      if (error instanceof AppError) throw error; // 이미 AppError면 그대로 throw
      throw new ExternalServiceError(SERVICE_NAME, error, '번개장터 API 호출 중 예기치 않은 오류');
    }
  );
};

handleApiResponseErrorInterceptor(bunjangApiClient);
handleApiResponseErrorInterceptor(bunjangCatalogApiClient);


/**
 * 번개장터 카탈로그 파일을 다운로드하고 압축을 해제하여 문자열로 반환합니다.
 * @param {string} filename - 다운로드할 카탈로그 파일명 (예: "full-20240524.csv.gz").
 * @returns {Promise<string>} 압축 해제된 CSV 데이터 문자열.
 * @throws {ExternalServiceError|AppError} 다운로드 또는 압축 해제 실패 시.
 */
async function downloadAndUnzipCatalogContent(filename) {
  logger.info(`[BunjangSvc] Downloading Bunjang catalog file: ${filename}`);
  try {
    const response = await bunjangCatalogApiClient.get(`/catalog/${filename}`, {
      responseType: 'arraybuffer', // gzip된 바이너리 데이터를 받기 위해
    });
    // 성공적인 응답 (200)은 인터셉터에서 이미 처리됨

    logger.info(`[BunjangSvc] Catalog file "${filename}" downloaded. Unzipping...`);
    return new Promise((resolve, reject) => {
      zlib.unzip(response.data, (err, buffer) => {
        if (err) {
          logger.error(`[BunjangSvc] Failed to unzip catalog "${filename}":`, err);
          return reject(new AppError(`카탈로그 파일 압축 해제 실패: ${filename}`, 500, 'CATALOG_UNZIP_ERROR', true, err));
        }
        logger.info(`[BunjangSvc] Catalog file "${filename}" unzipped successfully.`);
        resolve(buffer.toString('utf-8')); // CSV는 보통 UTF-8
      });
    });
  } catch (error) {
    // Axios 에러는 인터셉터에서 ExternalServiceError로 변환되어 throw됨
    logger.error(`[BunjangSvc] Error in downloadAndUnzipCatalogContent for "${filename}": ${error.message}`);
    if (error instanceof AppError || error instanceof ExternalServiceError) throw error;
    // 예기치 않은 에러
    throw new AppError(`카탈로그 콘텐츠 다운로드 및 압축 해제 중 오류 (${filename}): ${error.message}`, 500, 'CATALOG_DOWNLOAD_PROCESS_ERROR');
  }
}

/**
 * 번개장터 상품 상세 정보를 조회합니다. (Product Lookup API: /api/v1/products/{pid})
 * @param {string} pid - 조회할 번개장터 상품 ID.
 * @returns {Promise<object|null>} 번개장터 상품 상세 정보 객체, 또는 찾을 수 없거나 에러 시 null.
 */
async function getBunjangProductDetails(pid) {
  if (!pid) {
    logger.warn('[BunjangSvc] PID is required to fetch product details.');
    return null;
  }
  logger.debug(`[BunjangSvc] Fetching product details for Bunjang PID: ${pid}`);
  try {
    const response = await bunjangApiClient.get(`/api/v1/products/${pid}`);
    if (response.data && response.data.data) { // API 문서 기준, 실제 데이터는 response.data.data
      logger.info(`[BunjangSvc] Successfully fetched product details for PID ${pid}.`);
      return response.data.data;
    } else {
      logger.warn(`[BunjangSvc] No product data found in response for PID ${pid}.`, { responseData: response.data });
      return null; // 데이터 필드가 없는 경우
    }
  } catch (error) {
    // ExternalServiceError는 인터셉터에서 throw됨
    if (error instanceof ExternalServiceError && error.originalError?.response?.status === 404) {
      logger.info(`[BunjangSvc] Bunjang product with PID ${pid} not found (404).`);
      return null; // 404는 "없음"으로 간주하고 null 반환
    }
    logger.error(`[BunjangSvc] Failed to fetch Bunjang product details for PID ${pid}: ${error.message}`);
    // 그 외 에러는 null 반환 또는 에러를 다시 throw 할 수 있음
    // 여기서는 null 반환하여 호출 측에서 처리하도록 함
    return null;
  }
}

/**
 * 번개장터에 주문을 생성합니다. (Create Order V2 API: /api/v2/orders)
 * @param {object} orderPayload - 주문 생성 API 페이로드.
 * 예: { product: { id: number, price: number }, deliveryPrice: number }
 * @returns {Promise<object>} 번개장터 주문 생성 API 응답의 data 부분 (예: { id: newOrderId }).
 * @throws {ExternalServiceError|AppError} 주문 생성 실패 시.
 */
async function createBunjangOrderV2(orderPayload) {
  logger.info('[BunjangSvc] Attempting to create Bunjang order (V2):', { productId: orderPayload.product?.id });
  try {
    const response = await bunjangApiClient.post('/api/v2/orders', orderPayload);
    // 성공 시 API 문서 기준으로는 response.data.data 에 주문 ID가 있음
    if (response.data && response.data.data && response.data.data.id) {
      logger.info('[BunjangSvc] Successfully created Bunjang order (V2).', { 
        bunjangOrderId: response.data.data.id, productId: orderPayload.product?.id 
      });
      return response.data.data; // { id: newOrderId } 반환
    } else {
      logger.error('[BunjangSvc] Bunjang order creation response missing expected data.id.', { responseData: response.data });
      throw new AppError('번개장터 주문 생성 응답 형식이 유효하지 않습니다.', 500, 'BUNJANG_ORDER_RESPONSE_INVALID');
    }
  } catch (error) {
    // ExternalServiceError는 인터셉터에서 throw됨
    logger.error(`[BunjangSvc] Failed to create Bunjang order (V2) for product ID ${orderPayload.product?.id}: ${error.message}`);
    if (error instanceof AppError || error instanceof ExternalServiceError) throw error;
    throw new AppError(`번개장터 주문 생성 실패 (V2): ${error.message}`, 500, 'BUNJANG_ORDER_CREATE_V2_ERROR');
  }
}

// TODO: 번개장터 API의 다른 엔드포인트(카테고리 조회, 브랜드 조회, 주문 상태 조회 등) 함수 추가

module.exports = {
  // Axios 클라이언트 인스턴스를 직접 export 하기보다, 각 기능을 래핑한 함수를 제공하는 것이 좋음
  // bunjangApiClient,
  // bunjangCatalogApiClient,
  downloadAndUnzipCatalogContent,
  getBunjangProductDetails,
  createBunjangOrderV2,
};
