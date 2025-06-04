// src/jobs/queues.js
// BullMQ 큐 인스턴스를 생성하고 관리합니다.

const { Queue, Worker } = require('bullmq');
const config = require('../config');
const logger = require('../config/logger');
const { getBullMQRedisConnection } = require('../config/redisClient'); // BullMQ용 Redis 연결

const queues = {};

/**
 * 지정된 이름으로 BullMQ 큐를 생성하거나 가져옵니다.
 * @param {string} queueName - 큐 이름 (config.bullmq.queues 객체의 키 또는 값).
 * @returns {Queue} BullMQ 큐 인스턴스.
 */
function getQueue(queueName) {
  if (!config.redis.enabled) {
    logger.warn(`Redis is disabled. BullMQ queue "${queueName}" cannot be initialized.`);
    // Redis 비활성화 시 실제 큐 대신 모의 객체나 null 반환 고려 가능 (테스트 등)
    // 여기서는 에러를 발생시키거나, 호출하는 쪽에서 null 체크하도록 함
    return null;
  }

  if (!queues[queueName]) {
    logger.info(`Initializing BullMQ queue: ${queueName}`);
    queues[queueName] = new Queue(queueName, {
      connection: getBullMQRedisConnection(), // 각 큐는 자체 Redis 연결 사용 권장
      defaultJobOptions: config.bullmq.defaultJobOptions,
    });

    queues[queueName].on('error', (error) => {
      logger.error(`BullMQ queue "${queueName}" error:`, error);
    });
     queues[queueName].on('waiting', (jobId) => {
      // logger.debug(`Job ${jobId} is waiting in queue ${queueName}`);
    });
    queues[queueName].on('active', (job) => {
      // logger.debug(`Job ${job.id} is active in queue ${queueName}`);
    });
    queues[queueName].on('completed', (job, result) => {
      // logger.info(`Job ${job.id} in queue ${queueName} completed. Result: ${JSON.stringify(result)}`);
    });
    queues[queueName].on('failed', (job, err) => {
      logger.error(`Job ${job?.id} in queue ${queueName} failed. Error: ${err.message}`, {jobData: job?.data, stack: err.stack});
    });
  }
  return queues[queueName];
}

/**
 * 모든 활성 BullMQ 큐와 연결된 워커를 정상적으로 종료합니다.
 * 애플리케이션 종료 시 호출됩니다.
 */
async function closeAllQueues() {
  logger.info('Closing all BullMQ queues...');
  const closePromises = Object.values(queues).map(async (queue) => {
    if (queue) {
      try {
        await queue.close(); // 큐 연결 종료
        logger.info(`BullMQ queue "${queue.name}" closed.`);
      } catch (error) {
        logger.error(`Error closing BullMQ queue "${queue.name}":`, error);
      }
    }
  });
  await Promise.allSettled(closePromises);
  logger.info('All BullMQ queues have been requested to close.');
}

// 애플리케이션 시작 시점에 모든 필요한 큐를 미리 초기화할 수 있음
function initializeQueues() {
    if (!config.redis.enabled) return; // Redis 비활성화 시 큐 초기화 안함

    logger.info('Pre-initializing BullMQ queues defined in config...');
    Object.values(config.bullmq.queues).forEach(queueName => {
        getQueue(queueName); // 호출 시 내부적으로 생성 및 캐싱
    });
}


module.exports = {
  getQueue,
  initializeQueues, // index.js에서 호출하여 미리 큐 인스턴스 생성
  closeAllQueues,
};

