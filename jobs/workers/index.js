// src/jobs/workers/index.js
// 애플리케이션의 모든 BullMQ 워커를 초기화하고 관리합니다.

const config = require('../../config'); // 경로 수정
const logger = require('../../config/logger'); // 경로 수정
const { getBullMQRedisConnection } = require('../../config/redisClient'); // 경로 수정

// 각 워커 파일 import
const createExchangeRateWorker = require('./exchangeRateWorker');
const createCatalogWorker = require('./catalogWorker');
const createOrderWorker = require('./orderWorker');
// const createProductSyncWorker = require('./productSyncWorker'); // 필요시

const workers = []; // 활성 워커 인스턴스 저장 배열

/**
 * 모든 BullMQ 워커를 초기화하고 시작합니다.
 * 애플리케이션 시작 시 호출됩니다.
 */
function initializeAllWorkers() {
  if (!config.redis.enabled) {
    logger.warn('Redis is disabled. BullMQ workers will not be started.');
    return;
  }

  logger.info('[Workers] Initializing all BullMQ workers...');

  // 1. 환율 업데이트 워커
  const exchangeRateQueueName = config.bullmq.queues.exchangeRate;
  const exchangeRateWorker = createExchangeRateWorker(exchangeRateQueueName, getBullMQRedisConnection());
  workers.push(exchangeRateWorker);
  logger.info(`[Workers] Exchange Rate Worker for queue "${exchangeRateQueueName}" initialized.`);

  // 2. 카탈로그 처리 워커
  const catalogQueueName = config.bullmq.queues.catalog;
  const catalogWorker = createCatalogWorker(catalogQueueName, getBullMQRedisConnection());
  workers.push(catalogWorker);
  logger.info(`[Workers] Catalog Processing Worker for queue "${catalogQueueName}" initialized.`);
  
  // 3. 주문 처리 워커
  const orderQueueName = config.bullmq.queues.order;
  const orderWorker = createOrderWorker(orderQueueName, getBullMQRedisConnection());
  workers.push(orderWorker);
  logger.info(`[Workers] Order Processing Worker for queue "${orderQueueName}" initialized.`);

  // 4. (선택) 개별 상품 동기화 워커
  // const productSyncQueueName = config.bullmq.queues.productSync;
  // const productSyncWorker = createProductSyncWorker(productSyncQueueName, getBullMQRedisConnection());
  // workers.push(productSyncWorker);
  // logger.info(`[Workers] Product Sync Worker for queue "${productSyncQueueName}" initialized.`);


  logger.info(`[Workers] All ${workers.length} BullMQ workers have been initialized.`);
}

/**
 * 모든 활성 BullMQ 워커를 정상적으로 종료합니다.
 * 애플리케이션 종료 시 호출됩니다.
 */
async function closeAllWorkers() {
  if (workers.length === 0) {
    logger.info('[Workers] No active BullMQ workers to close.');
    return;
  }
  logger.info(`[Workers] Closing all ${workers.length} BullMQ workers...`);
  const closePromises = workers.map(async (worker) => {
    if (worker && typeof worker.close === 'function') {
      try {
        await worker.close(); // 각 워커의 close 메서드 호출
        logger.info(`[Workers] BullMQ worker for queue "${worker.name}" closed.`);
      } catch (error) {
        logger.error(`[Workers] Error closing BullMQ worker for queue "${worker.name}":`, error);
      }
    }
  });
  await Promise.allSettled(closePromises);
  workers.length = 0; // 배열 비우기
  logger.info('[Workers] All BullMQ workers have been requested to close.');
}

module.exports = {
  initializeAllWorkers,
  closeAllWorkers,
};
