// src/middleware/shopifyWebhookValidator.js
// Shopify 웹훅 요청의 HMAC-SHA256 서명을 검증합니다.

const crypto = require('crypto');
const config = require('../config');
const logger = require('../config/logger');
const { UnauthorizedError, ApiError } = require('../utils/customErrors');

/**
 * Shopify 웹훅 요청의 HMAC 서명을 검증하는 Express 미들웨어입니다.
 * 이 미들웨어는 반드시 raw request body에 접근할 수 있어야 합니다.
 * app.js에서 이 미들웨어를 사용하는 라우트 이전에 express.json() 같은
 * body-parser가 적용되지 않도록 하거나, bodyParser.raw()를 사용해야 합니다.
 */
function verifyShopifyWebhook(req, res, next) {
  const hmacHeader = req.get('X-Shopify-Hmac-Sha256');
  const shopDomain = req.get('X-Shopify-Shop-Domain'); // 어떤 상점에서 온 웹훅인지 로깅용
  const topic = req.get('X-Shopify-Topic'); // 어떤 토픽의 웹훅인지 로깅용

  if (!hmacHeader) {
    logger.warn('[WebhookValidator] HMAC signature is missing.', { shopDomain, topic, path: req.originalUrl });
    throw new UnauthorizedError('HMAC 서명이 누락되었습니다.', 'HMAC_MISSING');
  }

  // req.rawBody는 app.js에서 bodyParser.raw()를 통해 Buffer 형태로 설정되어야 합니다.
  const rawBody = req.rawBody;
  if (!rawBody || rawBody.length === 0) {
    logger.warn('[WebhookValidator] Raw body is missing or empty for HMAC verification.', { shopDomain, topic });
    throw new ApiError('HMAC 검증을 위한 원시 요청 본문이 없습니다.', 400, 'RAW_BODY_MISSING');
  }

  try {
    const generatedHash = crypto
      .createHmac('sha256', config.shopify.webhookSecret)
      .update(rawBody) // rawBody는 Buffer여야 함
      .digest('base64');

    // crypto.timingSafeEqual을 사용하여 타이밍 공격 방지
    const hmacBuffer = Buffer.from(hmacHeader);
    const generatedHashBuffer = Buffer.from(generatedHash);
    
    if (hmacBuffer.length === generatedHashBuffer.length && crypto.timingSafeEqual(hmacBuffer, generatedHashBuffer)) {
      // logger.debug('[WebhookValidator] Shopify webhook HMAC verification successful.', { shopDomain, topic });
      next(); // 서명 일치, 다음 핸들러로 진행
    } else {
      logger.warn('[WebhookValidator] Shopify webhook HMAC verification failed. Signatures do not match.', {
        shopDomain, topic, receivedHmac: hmacHeader.substring(0,10)+'...' , calculatedHmac: generatedHash.substring(0,10)+'...',
      });
      throw new UnauthorizedError('HMAC 서명 검증에 실패했습니다. 유효하지 않은 요청입니다.', 'HMAC_INVALID');
    }
  } catch (error) {
    logger.error('[WebhookValidator] Error during Shopify webhook HMAC verification:', { 
        message: error.message, shopDomain, topic, stack: error.stack 
    });
    if (error instanceof AppError) throw error; // 이미 AppError면 그대로 throw
    throw new AppError('HMAC 서명 검증 중 서버 오류가 발생했습니다.', 500, 'HMAC_VERIFICATION_ERROR');
  }
}

module.exports = verifyShopifyWebhook;
