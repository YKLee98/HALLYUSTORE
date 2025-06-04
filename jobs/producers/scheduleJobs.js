// src/jobs/producers/scheduleJobs.js
// node-cron을 사용하여 주기적으로 BullMQ에 작업을 추가(생산)합니다.

const cron = require('node-cron');
const config = require('../../config'); // ../../config 로 경로 수정
const logger = require('../../config/logger'); // ../../config 로 경로 수정
const { getQueue } = require('../queues'); // ../queues 로 경로 수정
const { AppError } = require('../../utils/customErrors'); // ../../utils 로 경로 수정

// 작업 데이터 생성 함수 (필요시 별도 파일로 분리)
const createJobData = {
  updateExchangeRates: () => ({ taskType: 'FETCH_AND_STORE_RATES' }),
  fetchFullCatalog: () => ({ catalogType: 'full', triggeredBy: 'cron_scheduler' }),
  fetchSegmentCatalog: () => {
    // 세그먼트 카탈로그는 특정 시간대의 파일명을 생성해야 할 수 있음
    // 예: const now = new Date(); const hour = now.getHours(); return { catalogType: 'segment', hour };
    return { catalogType: 'segment', triggeredBy: 'cron_scheduler' };
  },
};

/**
 * cron 표현식 유효성 검사 및 작업 스케줄링 헬퍼 함수
 * @param {string} cronExpression - Cron 표현식 문자열.
 * @param {string} jobName - 로깅 및 식별을 위한 작업 이름.
 * @param {string} queueName - 작업을 추가할 BullMQ 큐 이름.
 * @param {function} jobDataGenerator - 작업 데이터를 생성하는 함수.
 * @param {object} [jobOptions] - BullMQ 작업 옵션 (큐 기본값 오버라이드).
 */
function scheduleJob(cronExpression, jobName, queueName, jobDataGenerator, jobOptions = {}) {
  if (!config.redis.enabled) {
    logger.warn(`Redis is disabled, skipping schedule for job: ${jobName}`);
    return;
  }

  if (!cron.validate(cronExpression)) {
    logger.error(`Invalid cron expression for "${jobName}": ${cronExpression}. Job not scheduled.`);
    return;
  }

  const queue = getQueue(queueName);
  if (!queue) {
    logger.error(`Queue "${queueName}" not found for job "${jobName}". Job not scheduled.`);
    return;
  }

  cron.schedule(cronExpression, async () => {
    logger.info(`[Cron] Triggered job: "${jobName}" (cron: ${cronExpression}). Adding to queue: ${queueName}`);
    try {
      const jobData = jobDataGenerator();
      // 작업 ID는 BullMQ가 자동으로 생성하거나, 직접 지정 가능 (중복 방지 등 필요시)
      // const jobId = `${jobName.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}`;
      await queue.add(jobName, jobData, jobOptions); // 작업 이름과 데이터를 큐에 추가
      logger.info(`[Cron] Job "${jobName}" successfully added to queue "${queueName}". Data: ${JSON.stringify(jobData)}`);
    } catch (error) {
      logger.error(`[Cron] Failed to add job "${jobName}" to queue "${queueName}":`, error);
      // 여기서 AppError를 throw하면 cron 스케줄 자체가 멈출 수 있으므로 주의. 로깅만.
    }
  }, {
    scheduled: true,
    timezone: config.scheduler.timezone,
  });

  logger.info(`[Cron] Scheduled job: "${jobName}" with cron: ${cronExpression} on queue: ${queueName}`);
}

/**
 * 모든 예약된 작업을 초기화합니다.
 * 애플리케이션 시작 시 호출됩니다.
 */
function initializeScheduledJobs() {
  logger.info('[Scheduler] Initializing cron jobs for BullMQ producers...');

  // 1. 환율 정보 업데이트 작업
  scheduleJob(
    config.scheduler.updateExchangeRatesCron,
    'UpdateExchangeRates', // BullMQ 작업 이름 (워커에서 이 이름으로 처리)
    config.bullmq.queues.exchangeRate,
    createJobData.updateExchangeRates
  );

  // 2. 번개장터 전체 카탈로그 다운로드 작업
  scheduleJob(
    config.scheduler.fetchFullCatalogCron,
    'FetchBunjangCatalog-Full',
    config.bullmq.queues.catalog, // 카탈로그 처리 큐
    createJobData.fetchFullCatalog,
    { priority: 2 } // 예: 세그먼트보다 낮은 우선순위
  );

  // 3. 번개장터 시간별 업데이트 카탈로그 다운로드 작업
  scheduleJob(
    config.scheduler.fetchSegmentCatalogCron,
    'FetchBunjangCatalog-Segment',
    config.bullmq.queues.catalog,
    createJobData.fetchSegmentCatalog,
    { priority: 1 } // 예: 전체 카탈로그보다 높은 우선순위
  );
  
  // TODO: 여기에 다른 주기적인 작업들 추가 (예: 오래된 데이터 정리, 보고서 생성 등)

  logger.info('[Scheduler] All cron jobs for BullMQ producers have been scheduled.');
}

module.exports = initializeScheduledJobs;