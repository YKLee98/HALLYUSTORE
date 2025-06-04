// src/middleware/authMiddleware.js
// 내부 관리 API 엔드포인트 보호를 위한 간단한 API 키 기반 인증 미들웨어입니다.

const logger = require('../config/logger');
const config = require('../config');
const { UnauthorizedError, ForbiddenError, AppError } = require('../utils/customErrors');
const crypto = require('crypto'); // timingSafeEqual 사용 위해

const VALID_API_KEY = config.internalApiKey;

// 애플리케이션 시작 시점에 INTERNAL_API_KEY 설정 검증 (config/index.js의 validateRequiredConfig에서 이미 수행)
if (!VALID_API_KEY && config.env === 'production') {
  // 이 부분은 validateRequiredConfig에서 이미 process.exit(1)을 호출하여 도달하지 않아야 함.
  // 방어적으로 로깅.
  logger.fatal('CRITICAL SECURITY RISK: INTERNAL_API_KEY is not set in a production environment. Internal APIs are unprotected. Application should have exited.');
  // process.exit(1); // 여기서도 종료 가능
} else if (!VALID_API_KEY && config.env !== 'test') { // 테스트 환경에서는 키 없이도 통과 가능하도록 (주의)
    logger.warn(`SECURITY WARNING: INTERNAL_API_KEY is not set. Internal API endpoints will be accessible without authentication in ${config.env} mode. This is a SEVERE security risk if deployed to production or staging without a key.`);
} else if (VALID_API_KEY) {
    logger.info('[AuthMiddleware] Internal API Key authentication middleware is configured and active.');
}


/**
 * 요청 헤더 또는 쿼리 파라미터의 API 키를 검증합니다.
 * 타이밍 공격에 안전한 비교를 사용합니다.
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 * @param {import('express').NextFunction} next
 */
function verifyInternalApiKey(req, res, next) {
  // 테스트 환경이고 API 키가 설정 안됐으면 통과 (주의해서 사용)
  if (!VALID_API_KEY && config.env === 'test') {
    logger.warn(`[AuthMiddleware] No INTERNAL_API_KEY set, bypassing auth for ${req.method} ${req.originalUrl} in TEST mode.`);
    return next();
  }
  // VALID_API_KEY 자체가 설정 안된 경우 (개발 환경 포함, 프로덕션은 위에서 이미 에러 발생)
  if (!VALID_API_KEY) {
      logger.error('[AuthMiddleware] Server-side INTERNAL_API_KEY is not configured. Denying access.');
      throw new AppError('API 인증이 서버에 올바르게 설정되지 않았습니다. 관리자에게 문의하세요.', 500, 'AUTH_NOT_CONFIGURED_ON_SERVER');
  }

  const receivedApiKey = req.headers['x-api-key'] || req.query.apiKey;

  if (!receivedApiKey) {
    logger.warn(`[AuthMiddleware] API key missing in request to protected endpoint: ${req.method} ${req.originalUrl}`);
    throw new UnauthorizedError('API 키가 요청에 누락되었습니다. 헤더(x-api-key) 또는 쿼리 파라미터(apiKey)로 전달해야 합니다.', 'API_KEY_MISSING');
  }

  // 타이밍 공격 방지를 위해 crypto.timingSafeEqual 사용
  // 두 버퍼의 길이가 같아야 함
  const receivedApiKeyBuffer = Buffer.from(String(receivedApiKey)); // 항상 문자열로 변환 후 버퍼 생성
  const validApiKeyBuffer = Buffer.from(VALID_API_KEY);

  if (receivedApiKeyBuffer.length === validApiKeyBuffer.length && 
      crypto.timingSafeEqual(receivedApiKeyBuffer, validApiKeyBuffer)) {
    // logger.debug('[AuthMiddleware] Internal API key authentication successful.');
    return next();
  } else {
    logger.warn(`[AuthMiddleware] Invalid API key received for ${req.method} ${req.originalUrl}. Key starts with: ${String(receivedApiKey).substring(0,5)}...`);
    throw new ForbiddenError('제공된 API 키가 유효하지 않거나 권한이 없습니다.', 'API_KEY_INVALID_OR_FORBIDDEN');
  }
}

module.exports = {
  verifyInternalApiKey,
};
