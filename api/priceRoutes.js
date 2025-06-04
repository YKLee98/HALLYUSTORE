// src/api/priceRoutes.js
// 가격 계산 테스트 관련 API 라우트입니다.

const express = require('express');
const { query } = require('express-validator');
const priceController = require('../controllers/priceController');
const { handleValidationErrors } = require('../utils/validationHelper');
// const authMiddleware = require('../middleware/authMiddleware'); // 필요시 인증 적용

const router = express.Router();

// GET /api/price/calculate-shopify?krwPrice=10000[&krwShippingFee=3000]
// 주어진 KRW 가격에 대해 Shopify 리스팅 가격(USD)을 계산합니다.
router.get(
  '/calculate-shopify',
  // authMiddleware.verifyInternalApiKey, // 필요시 내부 API 키 인증 적용
  [ // 입력 유효성 검사 규칙
    query('krwPrice')
      .notEmpty().withMessage('krwPrice는 필수입니다.')
      .isFloat({ gt: 0 }).withMessage('krwPrice는 0보다 큰 숫자여야 합니다.')
      .toFloat(), // 숫자로 변환
    query('krwShippingFee')
      .optional() // 선택적 파라미터
      .isFloat({ gte: 0 }).withMessage('krwShippingFee는 0 이상의 숫자여야 합니다.')
      .toFloat(),
  ],
  handleValidationErrors, // 유효성 검사 결과 처리
  priceController.getCalculatedShopifyPrice
);

module.exports = router;
