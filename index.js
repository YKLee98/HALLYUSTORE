// src/index.js
// 애플리케이션의 주요 시작점입니다.
// 환경 설정 로드, 필수 설정 검증, DB/Redis 연결, 서버 시작, Graceful Shutdown 처리 등을 수행합니다.

// 'express-async-errors'는 try/catch 없이도 Express 라우트 핸들러의 비동기 에러를
// 자동으로 Express 에러 처리 미들웨어로 전달합니다. app.js보다 먼저 임포트하는 것이 좋습니다.
require('express-async-errors'); // 애플리케이션 최상단에서 한 번만 호출

const http = require('http');
const config = require('./config'); // 통합 설정 로더 (내부적으로 dotenv 호출)
const logger = require('./config/logger'); // Winston 로거 (config 의존)

// --- 필수 환경 변수 검증 (로거 초기화 후, 다른 모듈 로드 전) ---
try {
  config.validateRequiredConfig(logger);
  logger.info('[Startup] Configuration validation successful.');
} catch (configError) {
  // validateRequiredConfig가 process.exit(1)을 호출하므로, 이 catch는 이론상 도달 안 함.
  // 만약 validateRequiredConfig가 에러만 throw하도록 수정된다면 이 부분이 실행됨.
  logger.error('[Startup] CRITICAL: Configuration validation failed. Application cannot start.', configError); // logger.fatal -> logger.error
  process.exit(1);
}

const app = require('./app'); // Express 앱 (config, logger 의존)
const { connectDB, disconnectDB } = require('./config/database'); // DB 연결 (config, logger 의존)
const { getRedisClient, disconnectRedis } = require('./config/redisClient'); // Redis 연결 (config, logger 의존)
// src/jobs/index.js에서 통합 초기화/종료 함수 및 필요한 개별 함수들을 가져옵니다.
const { 
  initializeAllJobSystems, 
  shutdownAllJobSystems,
  // 필요에 따라 개별 제어 함수도 가져올 수 있으나, startServer에서는 initializeAllJobSystems를 사용합니다.
  // initializeQueues, 
  // closeAllQueues, 
  // initializeAllWorkers, // 이전 오류의 원인, initializeWorkers 대신 initializeAllWorkers가 jobs/index.js에 있음
  // closeAllWorkers 
} = require('./jobs');

const PORT = config.port;
const HOST = config.host;

const server = http.createServer(app); // Express 앱으로 HTTP 서버 생성

async function startServer() {
  logger.info(`[Startup] Starting ${config.appName} v${config.version} in ${config.env} mode...`);
  try {
    // 1. 데이터베이스 연결
    await connectDB();

    // 2. Redis 연결 (BullMQ는 자체 연결 사용, 여기서는 공유 클라이언트 초기화)
    if (config.redis.enabled) {
      getRedisClient(); // 호출 시 내부적으로 연결 시도 및 로깅
      logger.info('[Startup] Shared Redis client initialization requested (if enabled).');
    }

    // 3. BullMQ 큐, 워커 및 스케줄된 작업 초기화 (통합 함수 사용)
    if (config.redis.enabled) { // BullMQ는 Redis 필수
      initializeAllJobSystems(); // src/jobs/index.js 에 정의된 통합 초기화 함수 호출
      // 개별 로깅은 initializeAllJobSystems 내부에서 수행됨
    } else {
      logger.warn('[Startup] Redis is disabled, BullMQ job systems will not be started.');
    }
    
    // 4. HTTP 서버 시작
    server.listen(PORT, HOST, () => {
      logger.info(`[Startup] Server listening on http://${HOST}:${PORT}`);
      logger.info(`[Startup] Middleware base URL for webhooks/proxy: ${config.middlewareBaseUrl}`);
      logger.info(`[Startup] BullMQ Arena UI available at /admin/jobs (if enabled and not in production without auth).`);
      logger.info('[Startup] Application started successfully!');
    });

  } catch (error) {
    logger.error('[Startup] CRITICAL: Failed to start the server:', error); // logger.fatal -> logger.error
    // 서버 시작의 핵심 부분(DB, Redis 연결 등)에서 에러 발생 시,
    // gracefulShutdown을 호출하여 이미 시작된 리소스 정리 시도 후 종료.
    await gracefulShutdown('SERVER_STARTUP_FAILURE', error);
  }
}

// --- Graceful Shutdown 처리 ---
const signals = { 'SIGHUP': 1, 'SIGINT': 2, 'SIGTERM': 15 };
let isShuttingDown = false;

async function gracefulShutdown(signal, error) {
  if (isShuttingDown) {
    logger.warn(`[Shutdown] Already in progress. Ignoring signal: ${signal}`);
    return;
  }
  isShuttingDown = true;
  const exitCode = error ? 1 : (signals[signal] || 0);
  logger.info(`[Shutdown] Received ${signal}. Starting graceful shutdown (exit code: ${exitCode})...`);
  if (error) logger.error(`[Shutdown] Reason for shutdown:`, error);


  // 1. HTTP 서버부터 닫아서 새로운 요청을 받지 않도록 함
  logger.info('[Shutdown] Closing HTTP server...');
  server.close(async (closeErr) => {
    if (closeErr) logger.error('[Shutdown] Error closing HTTP server:', closeErr);
    else logger.info('[Shutdown] HTTP server closed.');

    // 2. BullMQ 관련 시스템 종료 (통합 함수 사용)
    if (config.redis.enabled) {
      await shutdownAllJobSystems(); // src/jobs/index.js 에 정의된 통합 종료 함수 호출
    }

    // 3. Redis 연결 종료 (공유 클라이언트)
    if (config.redis.enabled) {
      logger.info('[Shutdown] Disconnecting shared Redis client...');
      try { await disconnectRedis(); } catch (e) { logger.error('[Shutdown] Error disconnecting Redis:', e); }
    }

    // 4. MongoDB 연결 종료
    logger.info('[Shutdown] Disconnecting MongoDB...');
    try { await disconnectDB(); } catch (e) { logger.error('[Shutdown] Error disconnecting MongoDB:', e); }
    
    logger.info('[Shutdown] Graceful shutdown completed. Exiting.');
    process.exit(exitCode);
  });

  // 강제 종료 타이머
  const shutdownTimeout = setTimeout(() => {
    logger.warn('[Shutdown] Graceful shutdown timed out. Forcing exit.');
    process.exit(1); // 강제 종료
  }, 30 * 1000); // 예: 30초
  shutdownTimeout.unref(); // 타이머가 프로세스 활성 유지 않도록
}

Object.keys(signals).forEach((signal) => {
  process.on(signal, () => gracefulShutdown(signal));
});

process.on('unhandledRejection', (reason, promise) => {
  // AppError가 아닌 경우, 예상치 못한 에러이므로 심각하게 처리
  logger.error('[UnhandledRejection] Unhandled Rejection at:', promise, 'reason:', reason); // logger.fatal -> logger.error
  // 운영 환경에서는 에러 모니터링 시스템(Sentry 등)에 보고 후,
  // 상태가 불안정할 수 있으므로 graceful shutdown 후 종료 고려.
  gracefulShutdown('UNHANDLED_REJECTION', reason instanceof Error ? reason : new Error(String(reason)));
});

process.on('uncaughtException', (error) => {
  logger.error('[UncaughtException] Uncaught Exception:', error); // logger.fatal -> logger.error
  // 'uncaughtException'은 애플리케이션 상태가 매우 불안정함을 의미.
  // 로깅 후 즉시 graceful shutdown 시도 후 종료.
  gracefulShutdown('UNCAUGHT_EXCEPTION', error);
});

// --- 서버 시작 ---
startServer();