// src/controllers/shopifyAppProxyController.js
// Shopify App Proxy 요청을 처리하는 컨트롤러 로직

const config = require('../config');
const logger = require('../config/logger');
const SyncedProduct = require('../models/syncedProduct.model');
const { NotFoundError, ValidationError } = require('../utils/customErrors');
const { validationResult } = require('express-validator');

/**
 * GET /api/app-proxy/products
 * App Proxy를 통해 번개장터 연동 상품 목록을 필터링/검색하여 반환합니다.
 * Liquid 템플릿의 JavaScript에서 이 엔드포인트를 호출하여 상품 데이터를 가져옵니다.
 * 응답은 JSON 데이터이며, 클라이언트 측 JavaScript에서 렌더링합니다.
 */
async function getBunjangLinkedProducts(req, res, next) {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    throw new ValidationError('상품 목록 조회 요청 파라미터가 유효하지 않습니다.', errors.array());
  }

  // 쿼리 파라미터 (express-validator에서 sanitize/default 값 설정 가능)
  const {
    categories, // 쉼표로 구분된 번개장터 카테고리 ID 문자열
    search,     // 검색어
    page = 1,   // 페이지 번호 (기본값 1)
    limit = 20, // 페이지 당 상품 수 (기본값 20, 최대 100 등으로 제한)
    sort = 'latest', // 정렬 기준 (예: 'latest', 'price_asc', 'price_desc')
  } = req.query;

  logger.info('[AppProxyCtrlr /products] Request received:', { categories, search, page, limit, sort });

  try {
    const queryConditions = { syncStatus: 'SYNCED' }; // 성공적으로 Shopify에 동기화된 상품만 대상

    // 1. 카테고리 필터링
    // productMapper에서 BUNJANG_CATEGORY_TO_SHOPIFY_TYPE_XXX를 사용해 Shopify Product Type을 만들고,
    // 이를 SyncedProduct 모델의 'shopifyProductType' 필드에 저장했다고 가정.
    // 또는, 'bunjangCategoryId' 필드를 사용.
    if (categories) {
      const categoryIds = categories.split(',').map(id => id.trim()).filter(Boolean);
      if (categoryIds.length > 0) {
        // SyncedProduct 모델에 'bunjangCategoryId' 필드가 있다고 가정
        queryConditions.bunjangCategoryId = { $in: categoryIds };
        logger.debug('[AppProxyCtrlr /products] Applying category filter:', categoryIds);
      }
    } else {
      // categories 파라미터가 없으면, .env에 설정된 기본 필터 카테고리 사용
      const defaultFilterCategories = config.bunjang.filterCategoryIds;
      if (defaultFilterCategories.length > 0) {
        queryConditions.bunjangCategoryId = { $in: defaultFilterCategories };
        logger.debug('[AppProxyCtrlr /products] Applying default category filter:', defaultFilterCategories);
      }
    }

    // 2. 검색어 필터링
    if (search && search.trim() !== '') {
      const searchTerm = search.trim();
      // SyncedProduct 모델의 bunjangProductName, shopifyTitle, (Shopify) tags 필드 등에 대해 검색
      // MongoDB $text 인덱스 사용 시: queryConditions.$text = { $search: searchTerm };
      // 또는 $or 와 $regex 사용
      queryConditions.$or = [
        { bunjangProductName: { $regex: searchTerm, $options: 'i' } }, // 대소문자 무시
        { shopifyTitle: { $regex: searchTerm, $options: 'i' } }, // SyncedProduct에 shopifyTitle 저장 시
        // { shopifyTags: { $regex: searchTerm, $options: 'i' } }, // SyncedProduct에 shopifyTags 배열 저장 시
      ];
      logger.debug('[AppProxyCtrlr /products] Applying search term filter:', searchTerm);
    }

    // 3. 정렬
    let sortOption = { lastSuccessfulSyncAt: -1 }; // 기본: 최근 동기화된 순
    if (sort === 'price_asc') {
      sortOption = { shopifyListedPriceUsd: 1, lastSuccessfulSyncAt: -1 }; // 가격 오름차순 (숫자형으로 저장 필요)
    } else if (sort === 'price_desc') {
      sortOption = { shopifyListedPriceUsd: -1, lastSuccessfulSyncAt: -1 }; // 가격 내림차순
    } else if (sort === 'name_asc') {
        sortOption = { bunjangProductName: 1, lastSuccessfulSyncAt: -1 };
    }
    // 'latest'는 lastSuccessfulSyncAt 또는 bunjangUpdatedAt 기준

    // 4. 페이지네이션
    const pageNum = parseInt(page, 10);
    const limitNum = Math.min(parseInt(limit, 10), 100); // 최대 100개로 제한
    const skip = (pageNum - 1) * limitNum;

    const productsFromDb = await SyncedProduct.find(queryConditions)
      .sort(sortOption)
      .skip(skip)
      .limit(limitNum)
      .select('shopifyGid shopifyHandle bunjangProductName shopifyListedPriceUsd bunjangPid -_id') // 필요한 필드만 선택
      .lean();

    const totalProducts = await SyncedProduct.countDocuments(queryConditions);

    // 클라이언트에 전달할 상품 데이터 형식 구성
    const responseProducts = productsFromDb.map(p => ({
      id: p.shopifyGid, // Shopify Product GID
      handle: p.shopifyHandle,
      title: p.bunjangProductName, // 또는 Shopify 상품명 (syncedDoc에 shopifyTitle 저장 시)
      // 이미지는 Shopify CDN URL을 사용하는 것이 좋음 (productMapper에서 metafield 등으로 저장 후 사용)
      // 여기서는 임시로 플레이스홀더 또는 번개장터 PID 기반 URL 생성 (실제로는 부적합)
      imageUrl: `https://placehold.co/300x300/eee/ccc?text=PID-${p.bunjangPid}`,
      price: p.shopifyListedPriceUsd || 'N/A', // "25.99" 형태의 문자열
      // 상세 페이지 링크는 Shopify 상품 핸들을 사용
      // 주의: config.shopify.shopDomain은 "your-shop.myshopify.com" 형태여야 함. "hallyusuperstore.com"은 커스텀 도메인.
      // 실제로는 Shopify 스토어의 기본 URL을 사용하거나, 상대 경로만 제공하는 것이 나을 수 있음.
      url: `/products/${p.shopifyHandle}`, // Shopify 스토어 내 상대 경로
      bunjangPid: p.bunjangPid,
    }));

    // App Proxy 응답은 Content-Type: application/liquid 를 설정하면 Shopify가 Liquid 템플릿처럼 처리 가능.
    // 여기서는 JSON으로 응답하고, 클라이언트 측 JavaScript에서 렌더링.
    res.setHeader('Content-Type', 'application/json');
    res.status(200).json({
      products: responseProducts,
      pagination: {
        total: totalProducts,
        page: pageNum,
        limit: limitNum,
        totalPages: Math.ceil(totalProducts / limitNum),
        hasNextPage: pageNum * limitNum < totalProducts,
        hasPrevPage: pageNum > 1,
      }
    });

  } catch (error) {
    // AppError, ValidationError 등은 중앙 에러 핸들러에서 처리
    next(error);
  }
}

// TODO: 특정 상품 상세 정보 반환 엔드포인트 (/api/app-proxy/product/:bunjangPidOrShopifyHandle)
async function getBunjangLinkedProductDetail(req, res, next) {
    const { identifier } = req.params; // bunjangPid 또는 shopifyHandle
    logger.info(`[AppProxyCtrlr /product/:identifier] Request for identifier: ${identifier}`);
    // ... 로직 구현 ...
    next(new NotFoundError(`상품 상세 정보를 찾을 수 없습니다: ${identifier}`));
}


module.exports = {
  getBunjangLinkedProducts,
  getBunjangLinkedProductDetail,
};
