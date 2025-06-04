// src/api/index.js
// 애플리케이션의 모든 API 라우트를 통합하고 관리합니다.

const express = require('express');
const syncRoutes = require('./syncRoutes');
const priceRoutes = require('./priceRoutes'); // 가격 테스트 라우트 파일 분리
const shopifyAppProxyRoutes = require('./shopifyAppProxyRoutes');
const authMiddleware = require('../middleware/authMiddleware'); // 내부 API 인증용
const config = require('../config'); // App Proxy 경로 설정 읽기용

const router = express.Router();

// 기본 /api 경로 (헬스 체크 또는 API 정보)
router.get('/', (req, res) => {
  res.json({ 
    message: `Welcome to the ${config.appName} API`,
    version: config.version,
    status: 'healthy',
    timestamp: new Date().toISOString(),
  });
});

// 내부 관리/동기화 트리거용 라우트 (API 키 인증 적용)
router.use('/sync', authMiddleware.verifyInternalApiKey, syncRoutes);

// 가격 계산 테스트용 라우트 (개발/테스트 시에만 사용 권장, 필요시 인증 적용)
if (config.env !== 'production') { // 운영 환경에서는 비활성화 또는 인증 강화
    router.use('/price-utils', authMiddleware.verifyInternalApiKey, priceRoutes);
    // 또는 router.use('/price-utils', priceRoutes); // 인증 없이
}


// Shopify App Proxy 요청 처리 라우트
// Shopify Admin에서 설정한 App Proxy의 "Subpath prefix"가 여기에 반영되어야 함.
// 예: Shopify Admin에서 Subpath prefix = "bunjang" -> /apps/bunjang/*
//      Proxy URL = https://{미들웨어_BASE_URL}/api/proxy (여기서 /api/proxy 가 basePath)
// 이 라우터는 /api/proxy 로 마운트되고, 그 하위 경로 (예: /products)는 shopifyAppProxyRoutes에서 정의됨.
// Shopify에서 App Proxy URL을 `https://{host}/api/app-proxy`로 설정했다면,
// 이 미들웨어의 `/api/app-proxy` 경로로 요청이 오게 됨.
router.use(`/${config.shopify.appProxy.subpathPrefix || 'app-proxy'}`, shopifyAppProxyRoutes);
// 예: Shopify에서 /apps/bunjang-app 으로 설정했다면,
// router.use('/bunjang-app', shopifyAppProxyRoutes); // 이렇게 직접 매칭하거나,
// app.js에서 app.use(`/api/${config.shopify.appProxy.subpathPrefix}`, ...) 형태로 마운트.
// 현재는 /api/app-proxy 로 고정하고, Shopify Admin에서 Proxy URL을 여기에 맞춤.

// TODO: 기타 필요한 API 라우트 그룹 추가 (예: 사용자 관리, 설정 관리 등)

module.exports = router;
