// src/api/syncRoutes.js
// 동기화 작업을 수동으로 트리거하기 위한 API 라우트입니다.
// BullMQ 큐에 작업을 추가하는 방식으로 변경됩니다.

const express = require('express');
const router = express.Router();
const logger = require('../config/logger');
const config = require('../config');
const { getQueue } = require('../jobs/queues');
const { AppError } = require('../utils/customErrors');
// const productSyncController = require('../controllers/productSyncController'); // 컨트롤러 사용 시

// 이 라우트들은 authMiddleware.verifyInternalApiKey 를 통해 보호되어야 함 (api/index.js에서 적용)

/**
 * POST /api/sync/catalog/full
 * 전체 카탈로그 동기화 작업을 BullMQ에 추가합니다.
 */
router.post('/catalog/full', async (req, res, next) => {
  const jobName = 'ManualTrigger-FetchBunjangCatalog-Full';
  const queueName = config.bullmq.queues.catalog;
  logger.info(`[SyncRoute] API call to trigger full catalog sync. Adding to queue: ${queueName}`);
  
  if (!config.redis.enabled) {
    return next(new AppError('Redis is disabled, cannot add job to queue.', 503, 'QUEUE_DISABLED'));
  }
  const catalogQueue = getQueue(queueName);
  if (!catalogQueue) {
    return next(new AppError(`Queue "${queueName}" not available.`, 503, 'QUEUE_UNAVAILABLE'));
  }

  try {
    const jobData = { catalogType: 'full', triggeredBy: 'api_manual' };
    const job = await catalogQueue.add(jobName, jobData, {
      // jobId: `manual-full-catalog-${Date.now()}`, // 필요시 고유 ID
    });
    logger.info(`[SyncRoute] Job "${jobName}" (ID: ${job.id}) added to queue "${queueName}" for full catalog sync.`);
    res.status(202).json({ 
        message: '전체 카탈로그 동기화 작업이 큐에 추가되었습니다. 처리 상태는 서버 로그 또는 작업 대시보드를 확인하세요.',
        jobId: job.id,
        queueName: queueName,
    });
  } catch (error) {
    logger.error(`[SyncRoute] Error adding full catalog sync job to queue "${queueName}":`, error);
    next(new AppError('카탈로그 동기화 작업 추가 실패.', 500, 'QUEUE_JOB_ADD_FAILED', true, error));
  }
});

/**
 * POST /api/sync/catalog/segment
 * 세그먼트 카탈로그 동기화 작업을 BullMQ에 추가합니다.
 */
router.post('/catalog/segment', async (req, res, next) => {
  const jobName = 'ManualTrigger-FetchBunjangCatalog-Segment';
  const queueName = config.bullmq.queues.catalog;
  logger.info(`[SyncRoute] API call to trigger segment catalog sync. Adding to queue: ${queueName}`);

  if (!config.redis.enabled) return next(new AppError('Redis is disabled.', 503, 'QUEUE_DISABLED'));
  const catalogQueue = getQueue(queueName);
  if (!catalogQueue) return next(new AppError(`Queue "${queueName}" not available.`, 503, 'QUEUE_UNAVAILABLE'));

  try {
    const jobData = { catalogType: 'segment', triggeredBy: 'api_manual' };
    const job = await catalogQueue.add(jobName, jobData);
    logger.info(`[SyncRoute] Job "${jobName}" (ID: ${job.id}) added to queue "${queueName}" for segment catalog sync.`);
    res.status(202).json({
        message: '세그먼트 카탈로그 동기화 작업이 큐에 추가되었습니다.',
        jobId: job.id,
        queueName: queueName,
    });
  } catch (error) {
    logger.error(`[SyncRoute] Error adding segment catalog sync job to queue "${queueName}":`, error);
    next(new AppError('세그먼트 카탈로그 동기화 작업 추가 실패.', 500, 'QUEUE_JOB_ADD_FAILED', true, error));
  }
});


// TODO: 특정 Shopify 주문 재처리 엔드포인트 (Shopify Order ID를 받아 주문 처리 큐에 작업 추가)
// router.post('/order/:shopifyOrderId/reprocess', async (req, res, next) => { ... });

// TODO: 특정 번개장터 상품 재동기화 엔드포인트 (Bunjang PID를 받아 상품 동기화 큐에 작업 추가)
// router.post('/product/:bunjangPid/resync', async (req, res, next) => { ... });


module.exports = router;
