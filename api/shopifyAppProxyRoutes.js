// src/api/shopifyAppProxyRoutes.js
// Shopify App Proxy로부터 오는 요청을 처리합니다.

const express = require('express');
const crypto = require('crypto');
const { query, param } = require('express-validator'); // express-validator 사용
const config = require('../config');
const logger = require('../config/logger');
const shopifyAppProxyController = require('../controllers/shopifyAppProxyController');
const { handleValidationErrors } = require('../utils/validationHelper');
const { UnauthorizedError, ForbiddenError, AppError } = require('../utils/customErrors');

const router = express.Router();

/**
 * App Proxy 요청의 서명을 검증하는 미들웨어입니다.
 */
function verifyShopifyAppProxySignature(req, res, next) {
  const { signature, ...queryParamsWithoutSignature } = req.query; // signature를 제외한 모든 쿼리 파라미터

  if (!signature) {
    logger.warn('[AppProxyAuth] Signature missing from App Proxy request.');
    throw new UnauthorizedError('Shopify App Proxy 서명이 누락되었습니다.', 'PROXY_SIGNATURE_MISSING');
  }

  const sortedParams = Object.keys(queryParamsWithoutSignature)
    .sort()
    .map(key => `${key}=${Array.isArray(queryParamsWithoutSignature[key]) ? queryParamsWithoutSignature[key].join(',') : queryParamsWithoutSignature[key]}`)
    .join('');

  const calculatedSignature = crypto
    .createHmac('sha256', config.shopify.apiSecret) // Shopify API Secret Key 사용
    .update(Buffer.from(sortedParams, 'utf-8'))
    .digest('hex');

  if (crypto.timingSafeEqual(Buffer.from(calculatedSignature), Buffer.from(signature))) {
    // logger.debug('[AppProxyAuth] Shopify App Proxy signature verification successful.');
    // 검증된 파라미터만 req.verifiedQuery에 저장하여 컨트롤러에서 사용 (선택적)
    req.verifiedQuery = queryParamsWithoutSignature;
    next();
  } else {
    logger.warn('[AppProxyAuth] Shopify App Proxy signature verification failed.');
    throw new ForbiddenError('Shopify App Proxy 서명 검증에 실패했습니다.', 'PROXY_SIGNATURE_INVALID');
  }
}

// 모든 App Proxy 라우트에 서명 검증 미들웨어 적용
router.use(verifyShopifyAppProxySignature);

// 상품 목록 조회: GET /api/app-proxy/products
router.get(
  '/products',
  [ // 입력 유효성 검사 규칙
    query('categories').optional().isString().trim().matches(/^[\w,-]+$/).withMessage('카테고리 ID는 쉼표로 구분된 문자열이어야 합니다.'),
    query('search').optional().isString().trim().escape(), // XSS 방지 위해 escape
    query('page').optional().isInt({ min: 1 }).toInt().default(1),
    query('limit').optional().isInt({ min: 1, max: 100 }).toInt().default(20), // 최대 100개
    query('sort').optional().isIn(['latest', 'price_asc', 'price_desc', 'name_asc']).default('latest'),
  ],
  handleValidationErrors, // 유효성 검사 결과 처리
  shopifyAppProxyController.getBunjangLinkedProducts
);

// 특정 상품 상세 정보 조회: GET /api/app-proxy/product/:identifier
router.get(
  '/product/:identifier',
  [
    param('identifier').notEmpty().isString().trim().escape(),
    // 추가적인 identifier 형식 검증 (예: 숫자인지, 특정 패턴인지)
  ],
  handleValidationErrors,
  shopifyAppProxyController.getBunjangLinkedProductDetail
);

// TODO: 검색 제안(autocomplete) 엔드포인트
// router.get('/search-suggestions', ...);

module.exports = router;
