// src/controllers/productSyncController.js
// 상품 동기화 관련 API 요청을 처리하고, BullMQ 작업 큐에 작업을 추가합니다.

const logger = require('../config/logger');
const config = require('../config');
const { getQueue } = require('../jobs/queues');
const { AppError, ValidationError } = require('../utils/customErrors');
const { param, validationResult } = require('express-validator'); // 유효성 검사

/**
 * POST /api/sync/catalog/full
 * 전체 카탈로그 동기화 작업을 BullMQ에 추가합니다.
 */
async function triggerFullCatalogSync(req, res, next) {
  const jobName = 'ManualTrigger-FetchBunjangCatalog-Full'; // 작업 식별을 위한 이름
  const queueName = config.bullmq.queues.catalog;
  logger.info(`[ProductSyncCtrlr] API call to trigger full catalog sync. Adding to queue: ${queueName}`);
  
  if (!config.redis.enabled) { // Redis (BullMQ 의존성) 활성화 여부 확인
    return next(new AppError('Redis is disabled, catalog sync job cannot be queued.', 503, 'QUEUE_SYSTEM_DISABLED'));
  }
  const catalogQueue = getQueue(queueName);
  if (!catalogQueue) {
    return next(new AppError(`Catalog processing queue "${queueName}" is not available.`, 503, 'QUEUE_INSTANCE_UNAVAILABLE'));
  }

  try {
    const jobData = { catalogType: 'full', triggeredBy: 'api_manual_full_sync', requestedBy: req.ip }; // 요청자 IP 등 추가 정보
    const job = await catalogQueue.add(jobName, jobData, {
      jobId: `manual-full-catalog-${new Date().toISOString().split('T')[0]}`, // 하루에 한 번만 수동 실행되도록 ID 고정 (선택적)
      // priority: 2, // 작업 우선순위 (숫자가 낮을수록 높음)
    });
    logger.info(`[ProductSyncCtrlr] Job "${jobName}" (ID: ${job.id}) added to queue "${queueName}" for full catalog sync.`);
    res.status(202).json({ 
        message: '전체 카탈로그 동기화 작업이 성공적으로 큐에 추가되었습니다. 처리 상태는 서버 로그 또는 작업 대시보드를 확인하세요.',
        jobId: job.id,
        queueName: queueName,
        jobName: job.name,
        timestamp: new Date().toISOString(),
    });
  } catch (error) {
    logger.error(`[ProductSyncCtrlr] Error adding full catalog sync job to queue "${queueName}":`, error);
    next(new AppError('전체 카탈로그 동기화 작업 추가 중 오류가 발생했습니다.', 500, 'QUEUE_JOB_ADD_FAILED_FULL_CATALOG', true, error));
  }
}

/**
 * POST /api/sync/catalog/segment
 * 세그먼트 카탈로그 동기화 작업을 BullMQ에 추가합니다.
 */
async function triggerSegmentCatalogSync(req, res, next) {
  const jobName = 'ManualTrigger-FetchBunjangCatalog-Segment';
  const queueName = config.bullmq.queues.catalog;
  logger.info(`[ProductSyncCtrlr] API call to trigger segment catalog sync. Adding to queue: ${queueName}`);

  if (!config.redis.enabled) return next(new AppError('Redis is disabled.', 503, 'QUEUE_SYSTEM_DISABLED'));
  const catalogQueue = getQueue(queueName);
  if (!catalogQueue) return next(new AppError(`Queue "${queueName}" not available.`, 503, 'QUEUE_INSTANCE_UNAVAILABLE'));

  try {
    const jobData = { catalogType: 'segment', triggeredBy: 'api_manual_segment_sync', requestedBy: req.ip };
    const job = await catalogQueue.add(jobName, jobData, {
        // priority: 1,
    });
    logger.info(`[ProductSyncCtrlr] Job "${jobName}" (ID: ${job.id}) added to queue "${queueName}" for segment catalog sync.`);
    res.status(202).json({
        message: '세그먼트 카탈로그 동기화 작업이 큐에 추가되었습니다.',
        jobId: job.id,
        queueName: queueName,
        jobName: job.name,
        timestamp: new Date().toISOString(),
    });
  } catch (error) {
    logger.error(`[ProductSyncCtrlr] Error adding segment catalog sync job to queue "${queueName}":`, error);
    next(new AppError('세그먼트 카탈로그 동기화 작업 추가 실패.', 500, 'QUEUE_JOB_ADD_FAILED_SEGMENT_CATALOG', true, error));
  }
}

/**
 * POST /api/sync/product/:bunjangPid
 * 특정 번개장터 상품 ID를 받아 해당 상품만 재동기화하는 작업을 큐에 추가합니다.
 */
async function triggerSingleProductSync(req, res, next) {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    throw new ValidationError('잘못된 상품 ID 형식입니다.', errors.array());
  }

  const { bunjangPid } = req.params;
  const jobName = 'ManualTrigger-SyncSingleProduct';
  const queueName = config.bullmq.queues.productSync; // 개별 상품 동기화용 큐
  logger.info(`[ProductSyncCtrlr] API call to resync Bunjang product PID: ${bunjangPid}. Adding to queue: ${queueName}`);

  if (!config.redis.enabled) return next(new AppError('Redis is disabled.', 503, 'QUEUE_SYSTEM_DISABLED'));
  const productSyncQueue = getQueue(queueName);
  if (!productSyncQueue) return next(new AppError(`Queue "${queueName}" not available.`, 503, 'QUEUE_INSTANCE_UNAVAILABLE'));
  
  try {
    const jobData = { bunjangPid, triggeredBy: 'api_manual_single_product_sync', requestedBy: req.ip };
    // 동일 PID에 대한 중복 작업 방지 또는 고유 ID 생성
    const jobId = `manual-single-product-${bunjangPid}-${Date.now()}`; 
    const job = await productSyncQueue.add(jobName, jobData, { jobId });

    logger.info(`[ProductSyncCtrlr] Job "${jobName}" (ID: ${job.id}) for Bunjang PID ${bunjangPid} added to queue "${queueName}".`);
    res.status(202).json({
      message: `번개장터 상품(PID: ${bunjangPid}) 재동기화 작업이 큐에 추가되었습니다.`,
      jobId: job.id,
      queueName: queueName,
      jobName: job.name,
      bunjangPid,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    logger.error(`[ProductSyncCtrlr] Error adding single product sync job for PID ${bunjangPid} to queue "${queueName}":`, error);
    next(new AppError(`개별 상품(PID: ${bunjangPid}) 동기화 작업 추가 실패.`, 500, 'QUEUE_JOB_ADD_FAILED_SINGLE_PRODUCT', true, error));
  }
}


module.exports = {
  triggerFullCatalogSync,
  triggerSegmentCatalogSync,
  triggerSingleProductSync,
};
