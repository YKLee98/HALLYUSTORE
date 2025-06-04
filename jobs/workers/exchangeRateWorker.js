// src/jobs/workers/exchangeRateWorker.js
// BullMQ 워커: 환율 정보 업데이트 작업을 처리합니다.

const { Worker } = require('bullmq');
const config = require('../../config'); // 경로 수정
const logger = require('../../config/logger'); // 경로 수정
const { updateAndStoreExchangeRates } = require('../../services/exchangeRateService'); // 경로 수정
const { JobQueueError } = require('../../utils/customErrors'); // 경로 수정

const CONCURRENCY = parseInt(process.env.EXCHANGE_RATE_WORKER_CONCURRENCY, 10) || 1; // 동시 처리 작업 수

/**
 * 환율 업데이트 작업을 처리하는 워커 로직입니다.
 * @param {import('bullmq').Job} job - BullMQ 작업 객체.
 */
async function processExchangeRateUpdate(job) {
  logger.info(`[Worker: ${job.queueName}] Starting job ${job.id} (Name: ${job.name}). Data: ${JSON.stringify(job.data)}`);
  
  try {
    const result = await updateAndStoreExchangeRates(); // 서비스 함수 호출
    if (result) {
      logger.info(`[Worker: ${job.queueName}] Job ${job.id} completed successfully. Exchange rates updated.`);
      return { success: true, rates: { krwToUsd: result.krwToUsdRate, lastUpdated: result.lastUpdatedByApp } };
    } else {
      logger.warn(`[Worker: ${job.queueName}] Job ${job.id} completed, but exchange rates might not have been updated (check service logs).`);
      // 실패로 간주하지 않고, 서비스 로직에서 이미 로깅했다고 가정
      return { success: false, message: 'Exchange rate update service reported no changes or an issue.' };
    }
  } catch (error) {
    logger.error(`[Worker: ${job.queueName}] Job ${job.id} failed: ${error.message}`, {
      stack: error.stack, errorCode: error.errorCode, details: error.details,
    });
    // JobQueueError로 래핑하여 에러 타입 명확화
    throw new JobQueueError(job.queueName, { id: job.id, name: job.name, data: job.data }, error, `환율 업데이트 작업 실패 (Job ID: ${job.id})`);
  }
}

/**
 * 지정된 큐 이름과 Redis 연결을 사용하여 환율 업데이트 워커를 생성하고 시작합니다.
 * @param {string} queueName - 작업을 가져올 큐의 이름.
 * @param {object} connection - BullMQ용 Redis 연결 객체 (ioredis 인스턴스).
 * @returns {Worker} 생성된 BullMQ 워커 인스턴스.
 */
function createExchangeRateWorker(queueName, connection) {
  const worker = new Worker(queueName, processExchangeRateUpdate, {
    connection,
    concurrency: CONCURRENCY, // 동시에 처리할 작업 수
    limiter: { // 작업 처리 속도 제한 (선택 사항)
      max: 10, // 60초 동안 최대 10개 작업 처리
      duration: 60000,
    },
    // removeOnComplete: { age: 3600 * 24, count: 1000 }, // 기본값은 config.bullmq.defaultJobOptions 사용
    // removeOnFail: { age: 3600 * 24 * 7, count: 5000 },
  });

  worker.on('completed', (job, returnValue) => {
    logger.info(`[Worker: ${worker.name}] Job ${job.id} (Name: ${job.name}) completed. Return: ${JSON.stringify(returnValue)}`);
  });

  worker.on('failed', (job, error) => {
    logger.error(`[Worker: ${worker.name}] Job ${job?.id} (Name: ${job?.name}) failed ultimately: ${error.message}`, {
        jobData: job?.data,
        // stack: error.stack, // processExchangeRateUpdate에서 이미 상세 로깅
        attemptsMade: job?.attemptsMade,
    });
    // 여기에 추가적인 실패 알림 로직 (예: Sentry, 이메일) 구현 가능
  });

  worker.on('error', err => {
    logger.error(`[Worker: ${worker.name}] General error in worker:`, err);
  });
  
  logger.info(`[Worker] Exchange Rate Worker listening on queue "${queueName}" with concurrency ${CONCURRENCY}.`);
  return worker;
}

module.exports = createExchangeRateWorker;
