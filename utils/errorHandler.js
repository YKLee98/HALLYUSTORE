// src/utils/errorHandler.js
// 중앙 집중식 에러 핸들링 미들웨어입니다.
// Express 라우트 및 미들웨어에서 발생하는 모든 에러를 처리합니다.

const logger = require('../config/logger');
const config = require('../config');
const { AppError, ValidationError, ExternalServiceError } = require('./customErrors'); // customErrors.js 경로 확인

// eslint-disable-next-line no-unused-vars
function mainErrorHandler(err, req, res, next) {
  // 에러 로깅 (모든 에러를 로깅)
  // 운영 환경에서는 스택 트레이스를 응답에 포함하지 않도록 주의
  logger.error(`[ErrorHandler] Error occurred: ${err.message}`, {
    statusCode: err.statusCode || 500,
    errorCode: err.errorCode,
    path: req.originalUrl,
    method: req.method,
    ip: req.ip,
    isOperational: err.isOperational,
    details: err.details,
    // 운영 환경이 아닐 때만 스택 로깅 (또는 항상 로깅하되, 응답에는 제외)
    stack: config.env !== 'production' ? err.stack : undefined,
    // originalError: err.originalError ? err.originalError.message : undefined, // 필요시 원본 에러 메시지
  });

  // 이미 응답 헤더가 전송된 경우 Express 기본 에러 핸들러에 위임
  if (res.headersSent) {
    return next(err);
  }

  const statusCode = err.statusCode || 500;
  const responsePayload = {
    status: 'error',
    message: (err.isOperational && err.message) || '서버에서 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.',
    ...(err.errorCode && { errorCode: err.errorCode }), // errorCode가 있으면 포함
    ...(err.details && config.env !== 'production' && { details: err.details }), // 개발 시 상세 정보 포함
    ...(err instanceof ValidationError && { validationErrors: err.validationErrors }), // ValidationError인 경우 추가 정보
  };

  // 운영 환경이고, 운영 에러가 아니라면 (프로그래밍 에러 등) 일반적인 메시지로 대체
  if (config.env === 'production' && !err.isOperational) {
    responsePayload.message = '서버 내부 오류가 발생했습니다. 관리자에게 문의해주세요.';
    delete responsePayload.errorCode; // 내부 에러 코드는 숨김
    delete responsePayload.details;
  }

  res.status(statusCode).json(responsePayload);
}

module.exports = mainErrorHandler;