// src/services/catalogService.js

const fs = require('fs-extra');
const path = require('node:path');
const zlib = require('node:zlib');
const { pipeline } = require('node:stream/promises');
const axios = require('axios');
const csv = require('csv-parser');
const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');

const config = require('../config');
const logger = require('../config/logger');
const shopifyService = require('./shopifyService');
const SyncedProduct = require('../models/syncedProduct.model');
const { calculateShopifyPriceUsd } = require('./priceCalculationService');
const { AppError, ExternalServiceError } = require('../utils/customErrors');

const BUNJANG_COLLECTION_GID = 'gid://shopify/Collection/445888299257';
const TEMP_DOWNLOAD_DIR = config.tempDir || './tmp_downloads';

async function generateBunjangAuthHeader() {
  if (!config.bunjang.accessKey || !config.bunjang.secretKey) {
    logger.error('[CatalogSvc] Bunjang API Access Key or Secret Key is missing in configuration.');
    throw new AppError('Bunjang API credentials missing.', 500, 'BUNJANG_CREDENTIALS_MISSING');
  }
  try {
    const secretKeyDecoded = Buffer.from(config.bunjang.secretKey, 'base64');
    const payload = {
      accessKey: config.bunjang.accessKey,
      nonce: uuidv4(),
      iat: Math.floor(Date.now() / 1000),
    };
    const jwtToken = jwt.sign(payload, secretKeyDecoded, { algorithm: 'HS256' });
    return { 'Authorization': `Bearer ${jwtToken}` };
  } catch (error) {
    logger.error('[CatalogSvc] Failed to generate Bunjang JWT:', error);
    throw new AppError('Failed to generate Bunjang JWT.', 500, 'BUNJANG_JWT_ERROR', error);
  }
}

async function downloadAndProcessFile(fileUrl, downloadDir, baseOutputFileName, timeoutMs) {
  await fs.ensureDir(downloadDir);
  const tempDownloadedFilePath = path.join(downloadDir, `${baseOutputFileName}_${Date.now()}.tmp`);
  const finalCsvFilePath = path.join(downloadDir, `${baseOutputFileName}.csv`);

  logger.info(`[CatalogSvc] Attempting download of ${fileUrl}`);
  try {
    const authHeader = await generateBunjangAuthHeader();
    const response = await axios({
      method: 'get',
      url: fileUrl,
      headers: { ...authHeader },
      responseType: 'stream',
      timeout: timeoutMs || config.bunjang?.catalogDownloadTimeoutMs || 180000,
    });

    logger.info(`[CatalogSvc] Download request to ${fileUrl} - Status: ${response.status}`);
    const writer = fs.createWriteStream(tempDownloadedFilePath);
    await pipeline(response.data, writer);
    const stats = await fs.stat(tempDownloadedFilePath);
    logger.info(`[CatalogSvc] File successfully downloaded (raw): ${tempDownloadedFilePath}, Size: ${stats.size} bytes`);

    if (stats.size === 0) {
      await fs.remove(tempDownloadedFilePath);
      throw new Error(`Downloaded file ${tempDownloadedFilePath} is empty.`);
    }

    const contentEncoding = String(response.headers['content-encoding'] || '').toLowerCase();
    logger.info(`[CatalogSvc] Response Content-Encoding: '${contentEncoding}'`);

    if (contentEncoding === 'gzip' || contentEncoding === 'x-gzip') {
      logger.info(`[CatalogSvc] Content-Encoding is gzip. Unzipping ${tempDownloadedFilePath} to ${finalCsvFilePath}...`);
      const gunzip = zlib.createGunzip();
      const source = fs.createReadStream(tempDownloadedFilePath);
      const destination = fs.createWriteStream(finalCsvFilePath);
      await pipeline(source, gunzip, destination);
      logger.info(`[CatalogSvc] File unzipped successfully: ${finalCsvFilePath}`);
      await fs.remove(tempDownloadedFilePath);
    } else {
      logger.info(`[CatalogSvc] Content-Encoding not gzip. Assuming plain CSV. Moving ${tempDownloadedFilePath} to ${finalCsvFilePath}.`);
      await fs.move(tempDownloadedFilePath, finalCsvFilePath, { overwrite: true });
      logger.info(`[CatalogSvc] Plain CSV file moved to: ${finalCsvFilePath}`);
    }
    return finalCsvFilePath;
  } catch (error) {
    const responseStatus = error.response?.status;
    const errorMessage = error.response ? `Status: ${responseStatus}` : error.message;
    logger.error(`[CatalogSvc] Error during download/processing of ${fileUrl}: ${errorMessage}`, { stack: error.stack, responseStatus });
    await fs.remove(tempDownloadedFilePath).catch(err => logger.warn(`[CatalogSvc] Failed to remove temp download file on error: ${tempDownloadedFilePath}`, err));
    await fs.remove(finalCsvFilePath).catch(err => logger.warn(`[CatalogSvc] Failed to remove temp .csv file during error: ${finalCsvFilePath}`, err));
    throw new ExternalServiceError('BunjangCatalogProcessing', error, `번개장터 카탈로그 파일 처리 실패: ${fileUrl}. 원인: ${errorMessage}`);
  }
}

async function parseCsvFileWithRowProcessor(csvFilePath, rowProcessor) {
  const products = [];
  let rowNumber = 0;
  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        rowNumber++;
        const processedRow = rowProcessor(row, rowNumber);
        if (processedRow) {
          products.push(processedRow);
        }
      })
      .on('end', () => {
        logger.info(`[CatalogSvc] Successfully parsed and processed ${products.length} products (from ${rowNumber} CSV data rows) from ${csvFilePath}`);
        resolve({ products, totalRows: rowNumber });
      })
      .on('error', (error) => {
        logger.error(`[CatalogSvc] Error parsing CSV file ${csvFilePath}:`, error);
        reject(new AppError(`CSV 파일 파싱 오류: ${csvFilePath}`, 500, 'CSV_PARSE_ERROR', error));
      });
  });
}

function generateBunjangCatalogFilename(type, date = new Date()) {
  const year = date.getFullYear();
  const month = (date.getMonth() + 1).toString().padStart(2, '0');
  const day = date.getDate().toString().padStart(2, '0');
  if (type === 'full') {
    return `full-${year}${month}${day}.csv.gz`;
  } else if (type === 'segment') {
    const hour = date.getHours().toString().padStart(2, '0');
    return `segment-${year}${month}${day}_${hour}.csv.gz`;
  }
  throw new AppError('유효하지 않은 카탈로그 타입입니다.', 400, 'INVALID_CATALOG_TYPE');
}

function processCatalogRow(row, rowNumber) {
  const product = {
    pid: (row.pid || '').trim(),
    name: (row.name || '').trim(),
    description: (row.description || '').trim(),
    quantity: parseInt(row.quantity, 10),
    price: parseFloat(row.price),
    shippingFee: parseFloat(row.shippingFee || row.shipppingFee || 0),
    condition: (row.condition || 'USED').trim().toUpperCase(),
    saleStatus: (row.saleStatus || '').trim().toUpperCase(),
    keywords: row.keywords ? String(row.keywords).split(',').map(k => k.trim()).filter(Boolean) : [],
    images: row.images,
    categoryId: (row.categoryId || '').trim(),
    categoryName: (row.category_name || row.categoryName || '').trim(),
    brandId: (row.brandId || '').trim(),
    optionsRaw: row.options,
    uid: (row.uid || '').trim(),
    updatedAtString: row.updatedAt,
    createdAtString: row.createdAt,
  };

  try {
    if (product.updatedAtString) product.updatedAt = new Date(product.updatedAtString);
    if (product.createdAtString) product.createdAt = new Date(product.createdAtString);
    if ((product.updatedAtString && isNaN(product.updatedAt.getTime())) ||
        (product.createdAtString && isNaN(product.createdAt.getTime()))) {
      throw new Error('Invalid date format in CSV row');
    }
  } catch (e) {
    logger.warn(`[CatalogSvc] Invalid date for PID ${product.pid} (row #${rowNumber}). Dates set to null. updatedAt: "${product.updatedAtString}", createdAt: "${product.createdAtString}"`);
    product.updatedAt = null; product.createdAt = null;
  }

  if (product.saleStatus !== 'SELLING') {
    logger.debug(`[CatalogSvc] Row #${rowNumber} PID ${product.pid} skipped: saleStatus is '${product.saleStatus}' (not SELLING).`);
    return null;
  }
  if (!product.pid || !product.name || isNaN(product.price) || !product.updatedAt) {
    logger.warn(`[CatalogSvc] Row #${rowNumber} PID ${product.pid} skipped due to missing essential data (pid, name, price, or valid updatedAt).`);
    return null;
  }
  const filterCategoryIds = config.bunjang.filterCategoryIds || [];
  if (filterCategoryIds.length > 0 && product.categoryId && !filterCategoryIds.includes(product.categoryId)) {
    logger.debug(`[CatalogSvc] Row #${rowNumber} PID ${product.pid} skipped: categoryId '${product.categoryId}' not in filter list [${filterCategoryIds.join(', ')}].`);
    return null;
  }
  if (product.price < 0 || (!isNaN(product.quantity) && product.quantity < 0)) {
    logger.warn(`[CatalogSvc] Row #${rowNumber} PID ${product.pid} skipped due to invalid price (${product.price}) or quantity (${product.quantity}).`);
    return null;
  }
  return product;
}

function transformBunjangRowToShopifyInput(bunjangProduct, shopifyPriceUsd) {
  const tags = [`bunjang_import`, `bunjang_pid:${bunjangProduct.pid}`];
  const titleLower = (bunjangProduct.name || '').toLowerCase();
  const descriptionLower = (bunjangProduct.description || '').toLowerCase();
  const categoryLower = (bunjangProduct.categoryName || '').toLowerCase();

  const kpopKeywords = config.bunjang.kpopKeywords || [];
  const kidultKeywords = config.bunjang.kidultKeywords || [];

  if (kpopKeywords.length > 0 && kpopKeywords.some(keyword => titleLower.includes(keyword) || descriptionLower.includes(keyword) || categoryLower.includes(keyword))) {
    tags.push('K-Pop');
  }
  if (kidultKeywords.length > 0 && kidultKeywords.some(keyword => titleLower.includes(keyword) || descriptionLower.includes(keyword) || categoryLower.includes(keyword))) {
    tags.push('Kidult');
  }

  // 항상 ACTIVE 상태로 설정하여 바로 게시되도록 함
  // 제품은 생성/업데이트 후 자동으로 온라인 스토어에 게시됨
  let shopifyStatus = 'ACTIVE';
  
  const variantQuantity = !isNaN(parseInt(bunjangProduct.quantity, 10)) ? Math.max(0, parseInt(bunjangProduct.quantity, 10)) : 0;
  
  // Variant data - inventoryQuantities는 별도로 처리할 예정
  const variantData = {
    price: shopifyPriceUsd,
    sku: `BJ-${bunjangProduct.pid}`,
    inventoryPolicy: (variantQuantity > 0) ? 'DENY' : 'CONTINUE'
  };

  // 인벤토리 정보는 별도로 저장
  const inventoryInfo = {
    quantity: variantQuantity,
    locationId: config.shopify.defaultLocationId
  };

  if (bunjangProduct.optionsRaw) {
    try {
      let parsedOptions = [];
      if (typeof bunjangProduct.optionsRaw === 'string' && bunjangProduct.optionsRaw.trim()) {
        parsedOptions = JSON.parse(bunjangProduct.optionsRaw.trim());
      } else if (Array.isArray(bunjangProduct.optionsRaw)) {
        parsedOptions = bunjangProduct.optionsRaw;
      }
      if (Array.isArray(parsedOptions) && parsedOptions.length > 0 && parsedOptions[0].id && parsedOptions[0].value) {
        logger.info(`[CatalogSvc] Product PID ${bunjangProduct.pid} has Bunjang options: ${JSON.stringify(parsedOptions)}. Advanced variant/option mapping may be needed.`);
      }
    } catch (e) {
      logger.warn(`[CatalogSvc] Failed to parse Bunjang options for PID ${bunjangProduct.pid}: "${bunjangProduct.optionsRaw}"`, e);
    }
  }
  
  const productInput = {
    title: bunjangProduct.name,
    descriptionHtml: bunjangProduct.description || `Imported from Bunjang. Product ID: ${bunjangProduct.pid}`,
    vendor: config.bunjang.defaultVendor || "BunjangImport",
    productType: bunjangProduct.categoryName || config.bunjang.defaultShopifyProductType || "Uncategorized",
    tags: [...new Set(tags)],
    status: shopifyStatus,
    // Add publishedAt to ensure product is published
    publishedAt: new Date().toISOString()
  };
  
  logger.debug(`[CatalogSvc] ProductInput for PID ${bunjangProduct.pid}:`, { 
    title: productInput.title, 
    sku: variantData.sku,
    status: productInput.status
  });

  return { productInput, variantData, inventoryInfo };
}

async function syncBunjangProductToShopify(bunjangProduct, jobId = 'N/A') {
  const bunjangPid = bunjangProduct.pid;
  const bunjangName = bunjangProduct.name;
  const bunjangCatalogUpdatedAt = bunjangProduct.updatedAt;

  logger.info(`[CatalogSvc:Job-${jobId}] Syncing Bunjang PID: ${bunjangPid}, Name: ${bunjangName}`);
  let syncedDoc = await SyncedProduct.findOne({ bunjangPid }).lean();
  const now = new Date();

  await SyncedProduct.updateOne(
    { bunjangPid },
    {
      $set: {
        lastSyncAttemptAt: now,
        bunjangProductName: bunjangName,
        bunjangUpdatedAt: bunjangCatalogUpdatedAt,
        bunjangOriginalPriceKrw: bunjangProduct.price,
        bunjangOriginalShippingFeeKrw: bunjangProduct.shippingFee,
      },
      $inc: { syncAttemptCount: 1 },
      $setOnInsert: { bunjangPid, createdAt: now, syncStatus: 'PENDING' }
    },
    { upsert: true }
  );
  syncedDoc = await SyncedProduct.findOne({ bunjangPid }).lean();

  if (syncedDoc.syncStatus === 'SYNCED' &&
      bunjangCatalogUpdatedAt && syncedDoc.bunjangUpdatedAt &&
      new Date(syncedDoc.bunjangUpdatedAt).getTime() >= bunjangCatalogUpdatedAt.getTime() &&
      !config.forceResyncAll
      ) {
    logger.info(`[CatalogSvc:Job-${jobId}] Product ${bunjangPid} already SYNCED and no updates from Bunjang catalog (based on bunjangUpdatedAt). Skipping.`);
    return { status: 'skipped_no_change', message: 'Already synced and no update in catalog based on bunjangUpdatedAt.' };
  }

  let shopifyProductGid = syncedDoc.shopifyGid;
  if (!shopifyProductGid && bunjangPid) {
    try {
      const existingShopifyProduct = await shopifyService.findProductByBunjangPidTag(bunjangPid);
      if (existingShopifyProduct?.id) {
        shopifyProductGid = existingShopifyProduct.id;
        logger.info(`[CatalogSvc:Job-${jobId}] Found existing Shopify product ${shopifyProductGid} for Bunjang PID ${bunjangPid} via tag search.`);
      }
    } catch (tagSearchError) {
      logger.warn(`[CatalogSvc:Job-${jobId}] Error searching for existing product by tag for Bunjang PID ${bunjangPid}: ${tagSearchError.message}`);
      // Continue without existing product
    }
  }

  try {
    const shopifyPriceString = await calculateShopifyPriceUsd(bunjangProduct.price);
    const transformResult = transformBunjangRowToShopifyInput(bunjangProduct, shopifyPriceString);

    if (!transformResult || !transformResult.productInput) {
      logger.info(`[CatalogSvc:Job-${jobId}] Product PID ${bunjangPid} (Name: ${bunjangName}) skipped by transformBunjangRowToShopifyInput.`);
      await SyncedProduct.updateOne({ bunjangPid }, { $set: { syncStatus: 'SKIPPED_FILTER', lastSyncAttemptAt: now, bunjangUpdatedAt: bunjangCatalogUpdatedAt } });
      return { status: 'skipped_filter', message: 'Filtered out by transformation logic.' };
    }
    
    const { productInput: shopifyProductInput, variantData, inventoryInfo } = transformResult;

    let shopifyApiResult;
    let operationType = '';
    let createdOrUpdatedProductId = null;

    if (shopifyProductGid) {
      operationType = 'update';
      
      // Get existing variant ID for update
      let existingVariant = null;
      try {
        const existingProductResponse = await shopifyService.shopifyGraphqlRequest(`
          query getProduct($id: ID!) {
            product(id: $id) {
              variants(first: 1) {
                edges {
                  node {
                    id
                    inventoryItem {
                      id
                    }
                  }
                }
              }
            }
          }
        `, { id: shopifyProductGid });
        
        if (existingProductResponse?.data?.product?.variants?.edges && 
            existingProductResponse.data.product.variants.edges.length > 0 &&
            existingProductResponse.data.product.variants.edges[0]?.node?.id) {
          existingVariant = existingProductResponse.data.product.variants.edges[0].node;
        } else {
          logger.warn(`[CatalogSvc:Job-${jobId}] No existing variant found for product ${shopifyProductGid}. Will update product without variant data.`);
        }
      } catch (variantQueryError) {
        logger.error(`[CatalogSvc:Job-${jobId}] Failed to query existing variant for product ${shopifyProductGid}: ${variantQueryError.message}`);
        // Continue without variant update
      }
      
      logger.info(`[CatalogSvc:Job-${jobId}] Attempting to update Shopify product GID: ${shopifyProductGid}`);
      
      // Update product - NO variants in ProductInput
      const updateInput = {
        ...shopifyProductInput,
        id: shopifyProductGid
      };
      
      shopifyApiResult = await shopifyService.updateProduct(updateInput, BUNJANG_COLLECTION_GID, null);
      createdOrUpdatedProductId = shopifyApiResult?.id;
      
      // Update variant and inventory separately after product update
      if (existingVariant && existingVariant.id) {
        try {
          // Update variant price and SKU
          const variantUpdateData = {
            id: existingVariant.id,
            price: variantData.price,
            sku: variantData.sku,
            inventoryPolicy: variantData.inventoryPolicy
          };
          await shopifyService.updateProductVariant(variantUpdateData);
          logger.info(`[CatalogSvc:Job-${jobId}] Updated variant for product ${shopifyProductGid}`);
          
          // Update inventory if we have inventory item ID
          if (existingVariant.inventoryItem?.id && inventoryInfo.locationId && inventoryInfo.quantity >= 0) {
            try {
              await shopifyService.updateInventoryLevel(
                existingVariant.inventoryItem.id, 
                inventoryInfo.locationId, 
                inventoryInfo.quantity
              );
              logger.info(`[CatalogSvc:Job-${jobId}] Updated inventory for existing product ${shopifyProductGid}`);
            } catch (invError) {
              logger.error(`[CatalogSvc:Job-${jobId}] Failed to update inventory for ${shopifyProductGid}: ${invError.message}`);
              // Continue without failing the whole sync
            }
          }
        } catch (variantError) {
          logger.error(`[CatalogSvc:Job-${jobId}] Failed to update variant or inventory: ${variantError.message}`);
          // Continue without failing the whole sync
        }
      }
    } else {
      operationType = 'create';
      logger.info(`[CatalogSvc:Job-${jobId}] Attempting to create Shopify product for Bunjang PID: ${bunjangPid}`);
      
      // Create input WITHOUT variants
      const createInput = {
        ...shopifyProductInput
        // DO NOT include variants here
      };
      
      // Pass variant data as third parameter to createProduct
      const variantInfo = {
        price: variantData.price,
        sku: variantData.sku,
        inventoryPolicy: variantData.inventoryPolicy,
        quantity: inventoryInfo.quantity,
        locationId: inventoryInfo.locationId
      };
      
      logger.debug(`[CatalogSvc:Job-${jobId}] CreateInput structure (without variants):`, JSON.stringify(createInput, null, 2));
      logger.debug(`[CatalogSvc:Job-${jobId}] Variant info to be applied after creation:`, variantInfo);
      
      // Create product and handle variant separately
      shopifyApiResult = await shopifyService.createProduct(createInput, BUNJANG_COLLECTION_GID, variantInfo);
      createdOrUpdatedProductId = shopifyApiResult?.id;
    }

    if (!createdOrUpdatedProductId) {
      throw new Error('Shopify API did not return a valid product ID after create/update.');
    }

    // 이미지 첨부 단계
    const bunjangImageUrls = bunjangProduct.images;
    let mediaInputsToAttach = [];
    const productNameForAlt = bunjangProduct.name ? bunjangProduct.name.substring(0, 250) : 'Product image';

    // 이미지 URL 유효성 검사 및 변환 함수
    const processImageUrl = (url) => {
        if (!url || typeof url !== 'string') return null;
        let processedUrl = url.trim();
        
        // Ensure HTTPS for better Shopify compatibility
        if (processedUrl.startsWith('http://')) {
            processedUrl = processedUrl.replace('http://', 'https://');
            logger.debug(`[CatalogSvc:Job-${jobId}] Converted HTTP to HTTPS: ${processedUrl}`);
        }
        
        // Replace {res} placeholder with standard resolution
        if (processedUrl.includes('{res}')) {
            processedUrl = processedUrl.replace('{res}', '856');
            logger.debug(`[CatalogSvc:Job-${jobId}] Replaced {res} placeholder with 856 in URL: ${processedUrl}`);
        }
        
        // Basic URL validation
        if (!processedUrl.startsWith('https://')) return null;
        
        // 번개장터 이미지 서버 도메인 확인
        const bunjangDomains = ['media.bunjang.co.kr', 'img.bunjang.co.kr', 'img2.bunjang.co.kr'];
        try {
            const urlObj = new URL(processedUrl);
            const isBunjangUrl = bunjangDomains.some(domain => urlObj.hostname.includes(domain));
            
            // Accept Bunjang URLs (even though they might fail later) and standard image files
            if (isBunjangUrl) {
                logger.debug(`[CatalogSvc:Job-${jobId}] Bunjang image URL will be attempted: ${processedUrl}`);
                return processedUrl;
            }
            
            // For non-Bunjang URLs, check for image extensions
            if (/\.(jpg|jpeg|png|gif|webp|bmp)$/i.test(urlObj.pathname)) {
                return processedUrl;
            }
            
            // Also accept URLs without extensions if they're from known CDNs
            const knownCdns = ['cloudinary.com', 'imgix.net', 'amazonaws.com', 'googleusercontent.com'];
            if (knownCdns.some(cdn => urlObj.hostname.includes(cdn))) {
                return processedUrl;
            }
            
            return null;
        } catch (e) {
            return null;
        }
    };

    if (typeof bunjangImageUrls === 'string' && bunjangImageUrls.trim() !== '') {
        mediaInputsToAttach = bunjangImageUrls.split(',')
            .map(url => url.trim())
            .map(url => processImageUrl(url))
            .filter(url => {
                if (!url) {
                    logger.debug(`[CatalogSvc:Job-${jobId}] Invalid or unsupported image URL skipped`);
                    return false;
                }
                return true;
            })
            .map(url => ({ 
                originalSource: url, 
                mediaContentType: 'IMAGE', 
                alt: productNameForAlt 
            }));
    } else if (Array.isArray(bunjangImageUrls)) {
        mediaInputsToAttach = bunjangImageUrls
            .map(url => String(url || '').trim())
            .map(url => processImageUrl(url))
            .filter(url => {
                if (!url) {
                    logger.debug(`[CatalogSvc:Job-${jobId}] Invalid or unsupported image URL skipped`);
                    return false;
                }
                return true;
            })
            .map(url => ({ 
                originalSource: url, 
                mediaContentType: 'IMAGE', 
                alt: productNameForAlt 
            }));
    }
    
    if (mediaInputsToAttach.length > 0) {
        if (shopifyService.appendMediaToProduct) {
            try {
                logger.info(`[CatalogSvc:Job-${jobId}] Attaching ${mediaInputsToAttach.length} media items to product ${createdOrUpdatedProductId}`);
                const mediaResult = await shopifyService.appendMediaToProduct(createdOrUpdatedProductId, mediaInputsToAttach.slice(0, 250));
                
                // Log warning if some images failed (common with Bunjang URLs)
                if (mediaResult?.warning) {
                    logger.warn(`[CatalogSvc:Job-${jobId}] Media attachment warning: ${mediaResult.warning}`);
                }
                
                const successfulMedia = mediaResult?.media?.filter(m => m.status !== 'FAILED')?.length || 0;
                if (successfulMedia > 0) {
                    logger.info(`[CatalogSvc:Job-${jobId}] Successfully attached ${successfulMedia} media items to product.`);
                }
            } catch (mediaError) {
                // Don't fail the entire sync if media attachment fails
                logger.error(`[CatalogSvc:Job-${jobId}] Failed to attach media to product ${createdOrUpdatedProductId}: ${mediaError.message}`, { stack: mediaError.stack });
                // Continue with the sync - product is already created/updated
            }
        } else {
            logger.warn(`[CatalogSvc:Job-${jobId}] shopifyService.appendMediaToProduct function is not defined. Skipping media attachment for product ${createdOrUpdatedProductId}.`);
        }
    }

    await SyncedProduct.updateOne({ bunjangPid }, {
      $set: {
        shopifyGid: createdOrUpdatedProductId,
        shopifyProductId: createdOrUpdatedProductId.split('/').pop(),
        shopifyHandle: shopifyApiResult.handle,
        lastSuccessfulSyncAt: now,
        syncStatus: 'SYNCED',
        syncErrorMessage: null,
        syncErrorStackSample: null,
        shopifyListedPriceUsd: shopifyPriceString,
        bunjangUpdatedAt: bunjangCatalogUpdatedAt,
        syncAttemptCount: 0
      },
      $inc: { syncSuccessCount: 1 },
    });

    logger.info(`[CatalogSvc:Job-${jobId}] Successfully ${operationType}d Shopify product ${createdOrUpdatedProductId} for Bunjang PID ${bunjangPid}. Assigned to collection ${BUNJANG_COLLECTION_GID} and published to online store.`);
    return { status: 'success', operation: operationType, shopifyGid: createdOrUpdatedProductId };

  } catch (error) {
    let errorMessage = error.message;
    if (error.userErrors && Array.isArray(error.userErrors) && error.userErrors.length > 0) {
        errorMessage = error.userErrors.map(e => `Field: ${e.field?.join(',') || 'N/A'}, Msg: ${e.message}`).join('; ');
    } else if (error.networkError) {
        errorMessage = `Network error: ${error.message}`;
    }
    const errorStackSample = error.stack ? error.stack.substring(0, 1000) : null;
    logger.error(`[CatalogSvc:Job-${jobId}] Failed to ${shopifyProductGid ? 'update' : 'create'} Shopify product for Bunjang PID ${bunjangPid}: ${errorMessage}`, { originalErrorStack: error.originalError?.stack || error.stack });

    await SyncedProduct.updateOne({ bunjangPid }, {
      $set: {
        syncStatus: 'ERROR',
        syncErrorMessage: errorMessage.substring(0, 1000),
        syncErrorStackSample: errorStackSample,
        bunjangUpdatedAt: bunjangCatalogUpdatedAt,
        ...(shopifyProductGid && { shopifyGid: shopifyProductGid })
      }
    });
    return { status: 'error', message: errorMessage.substring(0, 255), shopifyGid: shopifyProductGid };
  }
}

async function fetchAndProcessBunjangCatalog(catalogType, jobIdForLog = 'N/A') {
  logger.info(`[CatalogSvc:Job-${jobIdForLog}] Starting Bunjang catalog processing. Type: ${catalogType}`);
  let catalogFileUrl;
  let catalogFileNameGz;
  let baseFileNameWithoutExt;

  const fileDate = new Date();
  catalogFileNameGz = generateBunjangCatalogFilename(catalogType, fileDate);
  baseFileNameWithoutExt = catalogFileNameGz.replace(/\.csv\.gz$/, '');

  if (!config.bunjang?.catalogApiUrl) {
    logger.error(`[CatalogSvc:Job-${jobIdForLog}] Bunjang catalog API URL (config.bunjang.catalogApiUrl) is not configured.`);
    throw new AppError("Bunjang catalog API URL is not configured.", 500, "BUNJANG_URL_MISSING");
  }
  catalogFileUrl = `${config.bunjang.catalogApiUrl}/catalog/${catalogType}/${catalogFileNameGz}`;
  logger.info(`[CatalogSvc:Job-${jobIdForLog}] Catalog file to process: ${catalogFileNameGz}, URL: ${catalogFileUrl}`);

  if (!TEMP_DOWNLOAD_DIR) {
    logger.error(`[CatalogSvc:Job-${jobIdForLog}] Temporary directory (TEMP_DOWNLOAD_DIR from config.tempDir) is not configured.`);
    throw new AppError("Temporary directory for downloads is not configured.", 500, "TEMP_DIR_MISSING");
  }
  const localCsvPath = await downloadAndProcessFile(catalogFileUrl, TEMP_DOWNLOAD_DIR, baseFileNameWithoutExt);
  logger.info(`[CatalogSvc:Job-${jobIdForLog}] Parsing CSV file: ${localCsvPath}`);
  const { products: bunjangProducts, totalRows: originalCsvRowCount } = await parseCsvFileWithRowProcessor(localCsvPath, processCatalogRow);

  if (!bunjangProducts || bunjangProducts.length === 0) {
    logger.warn(`[CatalogSvc:Job-${jobIdForLog}] No valid products found after filtering in CSV file: ${localCsvPath}. Processing finished.`);
    if (await fs.pathExists(localCsvPath)) await fs.remove(localCsvPath);
    return { filename: catalogFileNameGz, totalOriginalCsvRows: originalCsvRowCount || 0, validProductsToProcess: 0, successfullyProcessed: 0, errors: 0, skippedByFilter: 0, skippedNoChange: 0 };
  }

  let successfullyProcessed = 0;
  let errorCount = 0;
  let skippedByFilterCount = 0;
  let skippedNoChangeCount = 0;

  logger.info(`[CatalogSvc:Job-${jobIdForLog}] Processing ${bunjangProducts.length} valid items from Bunjang catalog...`);
  const concurrency = config.bunjang?.syncConcurrency || 1;
  const productChunks = [];
  for (let i = 0; i < bunjangProducts.length; i += concurrency) {
    productChunks.push(bunjangProducts.slice(i, i + concurrency));
  }

  for (const chunk of productChunks) {
    const chunkResults = await Promise.allSettled(
      chunk.map(product => syncBunjangProductToShopify(product, jobIdForLog))
    );
    chunkResults.forEach(result => {
      if (result.status === 'fulfilled' && result.value) {
        if (result.value.status === 'success') successfullyProcessed++;
        else if (result.value.status === 'skipped_filter') skippedByFilterCount++;
        else if (result.value.status === 'skipped_no_change') skippedNoChangeCount++;
        else if (result.value.status === 'error') errorCount++;
      } else if (result.status === 'rejected') {
        errorCount++;
        logger.error(`[CatalogSvc:Job-${jobIdForLog}] Unhandled promise rejection in sync chunk for a product:`, result.reason);
      }
    });
    logger.debug(`[CatalogSvc:Job-${jobIdForLog}] Processed a chunk. Totals - Success: ${successfullyProcessed}, FilterSkip: ${skippedByFilterCount}, NoChangeSkip: ${skippedNoChangeCount}, Errors: ${errorCount} / TotalValid: ${bunjangProducts.length}`);
  }

  if (await fs.pathExists(localCsvPath)) {
    await fs.remove(localCsvPath)
      .then(() => logger.info(`[CatalogSvc:Job-${jobIdForLog}] Cleaned up local CSV file: ${localCsvPath}`))
      .catch(unlinkError => logger.warn(`[CatalogSvc:Job-${jobIdForLog}] Failed to clean up local CSV file ${localCsvPath}:`, unlinkError));
  }

  const summary = {
    filename: catalogFileNameGz,
    totalOriginalCsvRows: originalCsvRowCount || 0,
    validProductsToProcess: bunjangProducts.length,
    successfullyProcessed,
    errors: errorCount,
    skippedByFilter: skippedByFilterCount,
    skippedNoChange: skippedNoChangeCount,
  };
  logger.info(`[CatalogSvc:Job-${jobIdForLog}] Bunjang catalog processing finished. Summary:`, summary);
  return summary;
}

module.exports = {
  fetchAndProcessBunjangCatalog,
};