// src/jobs/workers/orderWorker.js
// BullMQ 워커: Shopify 주문 웹훅 수신 후 번개장터 주문 생성 등의 작업을 처리합니다.

const { Worker } = require('bullmq');
const config = require('../../config'); // 경로 수정
const logger = require('../../config/logger'); // 경로 수정
const { processShopifyOrderForBunjang } = require('../../services/orderService'); // 경로 수정
const { JobQueueError } = require('../../utils/customErrors'); // 경로 수정

const CONCURRENCY = parseInt(process.env.ORDER_WORKER_CONCURRENCY, 10) || 5; // 주문 처리는 비교적 빠르므로 동시성 높게 가능

/**
 * Shopify 주문 처리 작업을 수행하는 워커 로직입니다.
 * @param {import('bullmq').Job} job - BullMQ 작업 객체. 작업 데이터는 Shopify 주문 객체.
 */
async function processOrderJob(job) {
  const shopifyOrder = job.data.shopifyOrder; // 작업 데이터에서 Shopify 주문 객체 가져오기
  const shopifyOrderId = shopifyOrder?.id || 'Unknown';
  logger.info(`[Worker: ${job.queueName}] Starting job ${job.id} (Name: ${job.name}) for Shopify Order ID: ${shopifyOrderId}`);

  if (!shopifyOrder || !shopifyOrder.id) {
    logger.error(`[Worker: ${job.queueName}] Job ${job.id} has invalid Shopify order data.`);
    throw new JobQueueError(job.queueName, job, null, '유효하지 않은 Shopify 주문 데이터입니다.');
  }

  try {
    await processShopifyOrderForBunjang(shopifyOrder); // 실제 주문 처리 서비스 호출
    logger.info(`[Worker: ${job.queueName}] Job ${job.id} for Shopify Order ID: ${shopifyOrderId} completed successfully.`);
    return { success: true, shopifyOrderId };
  } catch (error) {
    logger.error(`[Worker: ${job.queueName}] Job ${job.id} for Shopify Order ID: ${shopifyOrderId} failed: ${error.message}`, {
      stack: error.stack, errorCode: error.errorCode, details: error.details,
    });
    throw new JobQueueError(job.queueName, job, error, `Shopify 주문 처리 작업 실패 (Order ID: ${shopifyOrderId}, Job ID: ${job.id})`);
  }
}

function createOrderWorker(queueName, connection) {
  const worker = new Worker(queueName, processOrderJob, {
    connection,
    concurrency: CONCURRENCY,
  });

  worker.on('completed', (job, returnValue) => {
    logger.info(`[Worker: ${worker.name}] Job ${job.id} (Name: ${job.name}, ShopifyOrder: ${job.data.shopifyOrder?.id}) completed. Return: ${JSON.stringify(returnValue)}`);
  });

  worker.on('failed', (job, error) => {
    logger.error(`[Worker: ${worker.name}] Job ${job?.id} (Name: ${job?.name}, ShopifyOrder: ${job?.data.shopifyOrder?.id}) failed ultimately: ${error.message}`, {
        jobDataSummary: { shopifyOrderId: job?.data.shopifyOrder?.id, customerEmail: job?.data.shopifyOrder?.customer?.email }, // 민감 정보 제외
        attemptsMade: job?.attemptsMade,
    });
  });
  
  worker.on('error', err => {
    logger.error(`[Worker: ${worker.name}] General error in order worker:`, err);
  });

  logger.info(`[Worker] Order Processing Worker listening on queue "${queueName}" with concurrency ${CONCURRENCY}.`);
  return worker;
}

module.exports = createOrderWorker;
