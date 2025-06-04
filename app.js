// src/app.js
// Express 애플리케이션 설정 및 구성

// express-async-errors는 최상단에서 require 되어야 함 (index.js에서 이미 처리)
// require('express-async-errors');

const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const compression = require('compression');
const rateLimit = require('express-rate-limit');
const bodyParser = require('body-parser');
// const { Arena } = require('bull-arena');
const Bee = require('bullmq'); // Arena에서 BullMQ 타입 지정 위해 필요
const basicAuth = require('express-basic-auth'); // Arena UI Basic Auth
const { createBullBoard } = require('@bull-board/api');
const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter'); // Correct adapter for BullMQ
const { ExpressAdapter } = require('@bull-board/express');
const config = require('./config');
const logger = require('./config/logger');
const mainErrorHandler = require('./utils/errorHandler'); // 중앙 에러 핸들러
// const { handleValidationErrors } = require('./utils/validationHelper'); // 라우트에서 직접 사용

const shopifyWebhookValidator = require('./middleware/shopifyWebhookValidator');
const orderSyncController = require('./controllers/orderSyncController');
const apiRoutes = require('./api'); // 통합 API 라우터 (내부에 sync, price, app-proxy 라우트 포함)
const { getQueue } = require('./jobs/queues')
const app = express();

// --- 기본 보안 및 유틸리티 미들웨어 ---
// X-Powered-By 헤더 제거 등 기본 보안 설정
app.disable('x-powered-by'); 

app.use(helmet({
  contentSecurityPolicy: config.env === 'production' ? undefined : false, // 개발 중 CSP 완화
  crossOriginEmbedderPolicy: false, // Arena UI 등 외부 리소스 로드 위해 필요시 false
  crossOriginOpenerPolicy: { policy: "same-origin-allow-popups" }, // 필요시 조정
}));

// CORS 설정: 운영 환경에서는 특정 도메인만 허용하도록 강화
const corsOptions = {
  origin: (origin, callback) => {
    // 개발 환경이거나, 허용된 출처 목록에 있거나, origin이 없는 경우(예: Postman) 허용
    const allowedOrigins = (process.env.CORS_ALLOWED_ORIGINS || config.middlewareBaseUrl || '').split(',').map(o => o.trim()).filter(Boolean);
    if (config.env !== 'production' || !origin || allowedOrigins.includes(origin) || allowedOrigins.includes('*')) {
      callback(null, true);
    } else {
      logger.warn(`[CORS] Blocked request from origin: ${origin}`);
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true, // 쿠키/인증 헤더 공유 필요시 true
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Shopify-Hmac-Sha256', 'X-Api-Key', 'X-Request-ID'],
  exposedHeaders: ['Content-Length', 'X-Request-ID', 'RateLimit-Limit', 'RateLimit-Remaining', 'RateLimit-Reset', 'Retry-After'], // 클라이언트가 접근할 수 있는 응답 헤더
};
app.use(cors(corsOptions));
app.options('*', cors(corsOptions)); // Pre-flight 요청 처리

app.use(compression()); // 응답 압축

// HTTP 요청 로깅 (Morgan)
const morganFormat = config.env === 'production' ? 'short' : 'dev'; // 운영 시 로그 간소화
app.use(morgan(morganFormat, {
  stream: { write: (message) => logger.http(message.trim()) },
  skip: (req, res) => (res.statusCode < 400 && config.env === 'production'), // 운영 시 성공(2xx, 3xx) 요청은 로그 줄임
}));


// --- BullMQ Arena 대시보드 설정 ---
// --- BullMQ Arena 대시보드 설정 (BullBoard로 교체) ---
if (config.env !== 'production' && config.redis.enabled) {
  const serverAdapter = new ExpressAdapter(); // BullBoard Express Adapter
  serverAdapter.setBasePath('/admin/jobs');   // 대시보드 UI 경로

  // 설정 파일(config.bullmq.queues)에 정의된 모든 큐를 가져와서 BullBoard에 추가
  const queuesForBoard = Object.values(config.bullmq.queues).map(queueName => {
      const queueInstance = getQueue(queueName); // src/jobs/queues.js 에서 정의한 함수
      if (queueInstance) {
          return new BullMQAdapter(queueInstance); // 각 큐를 BullMQAdapter로 감쌉니다.
      }
      logger.warn(`[BullBoard] 큐 "${queueName}"의 인스턴스를 찾을 수 없어 대시보드에 추가하지 못했습니다.`);
      return null;
  }).filter(q => q !== null); // null인 경우 (큐 인스턴스를 못 찾은 경우) 제외

  if (queuesForBoard.length > 0) {
      createBullBoard({ // BullBoard 생성
          queues: queuesForBoard,
          serverAdapter: serverAdapter,
          options: { // (선택 사항) UI 추가 설정
              uiConfig: {
                  boardTitle: config.appName || 'Bunjang-Shopify Jobs',
                  // favIcon: { default: 'https://...', local: 'path/to/favicon.ico' }
              }
          }
      });

      // 대시보드 경로에 Basic Authentication 적용
      const arenaUsers = {}; // 변수명은 arenaUsers로 유지 (config 호환성)
      arenaUsers[config.bullmq.arenaAdmin.username] = config.bullmq.arenaAdmin.password;

      app.use('/admin/jobs', basicAuth({ users: arenaUsers, challenge: true, realm: 'BullBoardMonitor' }), serverAdapter.getRouter());
      logger.info(`Bull Board UI available at /admin/jobs. User: ${config.bullmq.arenaAdmin.username}`);
  } else {
      logger.warn('[BullBoard] 대시보드에 연결할 유효한 BullMQ 큐를 찾지 못했습니다.');
  }
} else if (config.env === 'production' && config.redis.enabled) {
    logger.info('Bull Board UI is typically disabled or access-restricted in production. Ensure strong authentication if enabled.');
}
// --- END BullMQ 대시보드 설정 ---


// --- Shopify 웹훅 전용 라우트 (raw body 파싱 필요) ---
const shopifyWebhookRouter = express.Router();
shopifyWebhookRouter.post(
  '/orders-create', // 예: /webhooks/shopify/orders-create
  bodyParser.raw({ type: 'application/json', verify: (req, res, buf) => { req.rawBody = buf; } }), // 1. raw body
  shopifyWebhookValidator, // 2. HMAC 검증
  orderSyncController.handleShopifyOrderCreateWebhook // 3. 컨트롤러
);
// 여기에 다른 Shopify 웹훅 핸들러 추가 (예: /orders-paid, /products-update)
// shopifyWebhookRouter.post('/orders-paid', ...);
app.use('/webhooks/shopify', shopifyWebhookRouter);


// --- 일반 API 요청을 위한 파서 및 라우트 ---
app.use(express.json({ limit: '10mb' })); // JSON 요청 본문 파싱
app.use(express.urlencoded({ extended: true, limit: '10mb' })); // URL-encoded 요청 본문 파싱

// API 요청 제한 (Rate Limiting) - 웹훅 경로는 제외
const apiLimiter = rateLimit({
  windowMs: config.rateLimit.windowMs,
  limit: config.rateLimit.max, // limit은 숫자 또는 함수여야 함
  standardHeaders: 'draft-7', 
  legacyHeaders: false,
  message: { error: '요청 한도를 초과했습니다. 잠시 후 다시 시도해 주세요.' },
  handler: (req, res, next, options) => {
    logger.warn(`Rate limit exceeded for IP ${req.ip || req.socket.remoteAddress}: ${options.message.error}`, { path: req.originalUrl });
    // res.setHeader('Retry-After', Math.ceil(config.rateLimit.windowMs / 1000)); // 초 단위로 Retry-After 설정
    res.status(options.statusCode).json(options.message);
  },
});
app.use('/api', apiLimiter); // '/api' 경로에만 적용

// 통합 API 라우터 마운트
app.use('/api', apiRoutes);


// --- 기본 헬스 체크 및 서비스 상태 라우트 ---
app.get('/', (req, res) => {
  res.status(200).json({
    application: config.appName,
    version: config.version,
    status: 'running',
    environment: config.env,
    timestamp: new Date().toISOString(),
  });
});
app.get('/health', async (req, res, next) => {
  // DB, Redis 등 주요 서비스 연결 상태 확인
  let dbHealthy = false;
  let redisHealthy = false;
  try {
    const mongoose = require('mongoose');
    dbHealthy = mongoose.connection.readyState === 1; // 1: connected

    if (config.redis.enabled) {
      const redisClient = require('./config/redisClient').getRedisClient();
      if (redisClient && redisClient.status === 'ready') {
        await redisClient.ping();
        redisHealthy = true;
      }
    } else {
      redisHealthy = true; // Redis 비활성화 시 건강한 것으로 간주
    }
    
    const overallHealthy = dbHealthy && redisHealthy;
    const statusCode = overallHealthy ? 200 : 503; // Service Unavailable

    res.status(statusCode).json({ 
        status: overallHealthy ? 'UP' : 'DEGRADED', 
        timestamp: new Date().toISOString(),
        dependencies: {
            database: dbHealthy ? 'UP' : 'DOWN',
            redis: config.redis.enabled ? (redisHealthy ? 'UP' : 'DOWN') : 'DISABLED',
        }
    });
  } catch (error) {
    logger.error('[HealthCheck] Error during health check:', error);
    next(new AppError('헬스 체크 중 오류 발생', 500, 'HEALTH_CHECK_ERROR', false, error));
  }
});


// --- 404 핸들러 (정의된 라우트가 없을 경우) ---
app.use((req, res, next) => {
  const { NotFoundError } = require('./utils/customErrors');
  next(new NotFoundError(`요청하신 API 엔드포인트 '${req.method} ${req.originalUrl}'를 찾을 수 없습니다.`));
});

// --- 중앙 집중식 에러 핸들러 (가장 마지막에 위치) ---
app.use(mainErrorHandler);

module.exports = app;
