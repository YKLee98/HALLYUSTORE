// src/jobs/index.js
// 이 파일은 BullMQ 관련 모듈(큐, 워커, 생산자)의 주요 함수들을 통합하여 내보냅니다.
// 애플리케이션의 메인 시작점(src/index.js)에서 이 파일을 통해 작업 관련 기능을 초기화하고 관리합니다.

const logger = require('../config/logger'); // ../config/logger.js
const { initializeQueues, closeAllQueues, getQueue } = require('./queues');
const { initializeAllWorkers, closeAllWorkers } = require('./workers'); // ./workers/index.js
const initializeScheduledJobs = require('./producers/scheduleJobs'); // ./producers/scheduleJobs.js

/**
 * 모든 BullMQ 관련 컴포넌트(큐, 워커, 스케줄된 작업 생산자)를 초기화합니다.
 * 이 함수는 애플리케이션 시작 시 호출되어야 합니다.
 */
function initializeAllJobSystems() {
  logger.info('[JobSystems] Initializing all BullMQ job systems...');
  
  // 1. 큐 인스턴스 생성 및 초기화
  // initializeQueues()는 config에 정의된 모든 큐를 미리 생성합니다.
  initializeQueues(); 
  logger.info('[JobSystems] BullMQ queues pre-initialized.');

  // 2. 워커 초기화 및 시작
  // initializeAllWorkers()는 각 큐에 대한 워커를 생성하고 리스닝을 시작합니다.
  initializeAllWorkers();
  logger.info('[JobSystems] BullMQ workers initialized and started.');

  // 3. 스케줄된 작업 생산자(Cron Jobs) 초기화
  // initializeScheduledJobs()는 node-cron을 사용하여 주기적으로
  // BullMQ 큐에 작업을 추가하는 스케줄러를 설정합니다.
  initializeScheduledJobs();
  logger.info('[JobSystems] Scheduled job producers (cron jobs for BullMQ) initialized.');

  logger.info('[JobSystems] All BullMQ job systems have been initialized.');
}

/**
 * 모든 BullMQ 관련 컴포넌트를 정상적으로 종료합니다.
 * 이 함수는 애플리케이션 종료(Graceful Shutdown) 시 호출되어야 합니다.
 */
async function shutdownAllJobSystems() {
  logger.info('[JobSystems] Shutting down all BullMQ job systems...');

  // 1. 워커 종료 (새 작업 수신 중단 및 현재 작업 완료 대기)
  logger.info('[JobSystems] Requesting all BullMQ workers to close...');
  await closeAllWorkers();
  logger.info('[JobSystems] All BullMQ workers have been requested to close.');

  // 2. 큐 연결 종료
  logger.info('[JobSystems] Requesting all BullMQ queues to close...');
  await closeAllQueues();
  logger.info('[JobSystems] All BullMQ queues have been requested to close.');
  
  // Cron 작업은 별도로 중지할 필요가 없음 (프로세스 종료 시 함께 종료됨)
  // 만약 cron 인스턴스를 배열 등으로 관리했다면, 각 인스턴스의 .stop() 메서드 호출 가능

  logger.info('[JobSystems] All BullMQ job systems have been shut down.');
}


module.exports = {
  // 초기화 및 종료 함수
  initializeAllJobSystems, // src/index.js 에서 호출하여 전체 작업 시스템 시작
  shutdownAllJobSystems,   // src/index.js 의 gracefulShutdown에서 호출

  // 개별 모듈의 함수들도 필요에 따라 직접 내보내기 가능 (보통은 위 통합 함수 사용)
  // Queues
  initializeQueues,     // 개별 큐 초기화 (이미 initializeAllJobSystems에 포함)
  closeAllQueues,       // 개별 큐 종료 (이미 shutdownAllJobSystems에 포함)
  getQueue,             // 특정 큐 인스턴스 직접 가져오기 (예: API 컨트롤러에서 작업 추가 시)

  // Workers
  initializeAllWorkers, // 개별 워커 초기화 (이미 initializeAllJobSystems에 포함)
  closeAllWorkers,      // 개별 워커 종료 (이미 shutdownAllJobSystems에 포함)

  // Producers (Cron Schedulers)
  initializeScheduledJobs, // 개별 스케줄러 초기화 (이미 initializeAllJobSystems에 포함)
};
