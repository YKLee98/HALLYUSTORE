// src/controllers/orderSyncController.js
// Shopify 주문 웹훅을 수신하여 BullMQ 작업 큐에 주문 처리 작업을 추가합니다.

const config = require('../config');
const logger = require('../config/logger');
const { getQueue } = require('../jobs/queues'); // BullMQ 큐 가져오기
const { ApiError, AppError } = require('../utils/customErrors');

/**
 * Shopify 'orders/create' 또는 'orders/paid' 웹훅을 처리합니다.
 * 유효한 주문이면 BullMQ의 주문 처리 큐에 작업을 추가합니다.
 * @param {import('express').Request} req - Express 요청 객체 (rawBody 포함).
 * @param {import('express').Response} res - Express 응답 객체.
 * @param {import('express').NextFunction} next - Express next 미들웨어 함수.
 */
async function handleShopifyOrderCreateWebhook(req, res, next) {
  let shopifyOrder;
  try {
    // rawBody는 shopifyWebhookValidator 미들웨어 이전에 bodyParser.raw()에 의해 Buffer로 설정됨
    if (!req.rawBody) {
        throw new ApiError('Webhook raw body is missing.', 400, 'RAW_BODY_MISSING_FOR_PARSING');
    }
    shopifyOrder = JSON.parse(req.rawBody.toString('utf8'));
  } catch (parseError) {
    logger.error('[OrderSyncCtrlr] Failed to parse Shopify order webhook payload:', parseError);
    // throw new ApiError('Webhook payload parsing error.', 400, 'WEBHOOK_PAYLOAD_PARSE_ERROR');
    // 파싱 에러 시 200을 보내 Shopify 재전송 루프를 막을 수도 있지만, 여기서는 에러로 처리
    return res.status(400).json({ error: 'Invalid webhook payload.' });
  }

  const shopifyOrderId = shopifyOrder?.id || 'Unknown';
  const financialStatus = shopifyOrder?.financial_status;
  const shopDomain = req.get('X-Shopify-Shop-Domain');

  logger.info(`[OrderSyncCtrlr] Received Shopify order webhook for Order ID: ${shopifyOrderId} from ${shopDomain}. Financial Status: ${financialStatus}`);

  // 결제 완료된 주문만 처리 (또는 'orders/paid' 웹훅을 별도로 구독)
  // 'paid', 'partially_paid' 상태를 처리 대상으로 간주. 'authorized'는 아직 결제 확정 아님.
  if (financialStatus === 'paid' || financialStatus === 'partially_paid') {
    if (!config.redis.enabled) {
        logger.error(`[OrderSyncCtrlr] Redis is disabled. Cannot add order ${shopifyOrderId} to BullMQ queue. Processing will be skipped.`);
        // Redis 비활성화 시 바로 200 OK 보내고 무시하거나, 동기 처리 시도 (비권장)
        return res.status(200).send('Webhook received, but job queue is disabled. Order processing skipped.');
    }

    const orderQueueName = config.bullmq.queues.order;
    const orderQueue = getQueue(orderQueueName);

    if (!orderQueue) {
        logger.error(`[OrderSyncCtrlr] BullMQ order queue "${orderQueueName}" is not available. Cannot process order ${shopifyOrderId}.`);
        // 큐가 없으면 심각한 문제. 500 에러 또는 200 OK 후 관리자 알림.
        return res.status(500).send('Order processing queue unavailable.');
    }

    try {
      // 작업 데이터에는 전체 Shopify 주문 객체 또는 필요한 부분만 포함
      const jobData = { shopifyOrder, receivedAt: new Date().toISOString(), sourceShop: shopDomain };
      // 작업 ID는 Shopify 주문 ID를 사용하여 중복 추가 방지 (BullMQ는 동일 ID 작업 추가 시 무시 또는 업데이트 가능)
      const jobId = `shopify-order-${shopifyOrderId}`; 
      
      await orderQueue.add('ProcessShopifyOrder', jobData, { 
        jobId, // 중복 방지용 작업 ID
        // removeOnComplete: true, // 바로 삭제 (기본 옵션 따름)
        // attempts: 5, // 이 작업에 대한 특정 재시도 횟수
      });

      logger.info(`[OrderSyncCtrlr] Shopify Order ID: ${shopifyOrderId} successfully added to queue "${orderQueueName}" with Job ID: ${jobId}.`);
      // Shopify 웹훅은 빠른 응답(200 OK)을 기대함
      res.status(200).send('Webhook received and order queued for processing.');
    } catch (queueError) {
      logger.error(`[OrderSyncCtrlr] Failed to add Shopify Order ID: ${shopifyOrderId} to queue "${orderQueueName}":`, queueError);
      // 큐 추가 실패 시 503 Service Unavailable 등으로 응답하여 Shopify가 재시도하도록 유도 가능
      // 또는 200 OK 보내고 내부적으로 재시도/알림 처리 (선택)
      next(new AppError(`주문 처리 큐에 작업 추가 실패 (Order ID: ${shopifyOrderId})`, 503, 'QUEUE_ADD_FAILED', true, queueError));
    }
  } else {
    logger.info(`[OrderSyncCtrlr] Shopify Order ID: ${shopifyOrderId} financial_status is '${financialStatus}'. Skipping queueing for Bunjang order creation.`);
    res.status(200).send('Webhook received, order not in processable payment status.');
  }
}

module.exports = {
  handleShopifyOrderCreateWebhook,
};
