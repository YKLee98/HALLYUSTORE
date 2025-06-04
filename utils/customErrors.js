// src/utils/customErrors.js
// 애플리케이션 전반에서 사용할 커스텀 에러 클래스들을 정의합니다.

/**
 * 기본 애플리케이션 에러 클래스입니다.
 * 모든 커스텀 에러는 이 클래스를 상속받습니다.
 */
class AppError extends Error {
  constructor(message, statusCode, errorCode, isOperational = true, details = null) {
    super(message);
    this.statusCode = statusCode || 500;
    this.errorCode = errorCode || 'INTERNAL_SERVER_ERROR'; // 내부 에러 코드 (API 응답용)
    this.isOperational = isOperational; // 운영상 예상된 에러인지 (프로그래밍 에러와 구분)
    this.details = details; // 추가적인 에러 정보 (객체 또는 배열)

    // Error.captureStackTrace(this, this.constructor); // V8 환경에서 스택 트레이스 캡처
    // Node.js에서는 Error 생성자가 자동으로 스택을 설정하므로 위 라인은 보통 불필요.
    // 만약 스택 트레이스에서 생성자 호출 부분을 제외하고 싶다면 사용할 수 있습니다.
  }
}

/**
 * API 요청 관련 에러 (4xx대)
 */
class ApiError extends AppError {
  constructor(message, statusCode = 400, errorCode = 'API_ERROR', details = null) {
    super(message, statusCode, errorCode, true, details);
  }
}

/**
 * 입력값 유효성 검사 에러 (400 또는 422)
 */
class ValidationError extends ApiError {
  constructor(message = '입력값이 유효하지 않습니다.', errors = [], statusCode = 422, errorCode = 'VALIDATION_FAILED') {
    // errors는 express-validator 등의 결과 배열일 수 있음
    super(message, statusCode, errorCode, errors); // details에 errors 배열 전달
    this.errors = errors; // 편의를 위해 직접 접근 가능한 속성으로도 추가
  }
}

/**
 * 인증 실패 에러 (401)
 */
class UnauthorizedError extends ApiError {
  constructor(message = '인증에 실패했습니다. 유효한 자격 증명이 필요합니다.', errorCode = 'UNAUTHORIZED') {
    super(message, 401, errorCode);
  }
}

/**
 * 권한 없음 에러 (403)
 */
class ForbiddenError extends ApiError {
  constructor(message = '요청한 리소스에 접근할 권한이 없습니다.', errorCode = 'FORBIDDEN') {
    super(message, 403, errorCode);
  }
}

/**
 * 리소스를 찾을 수 없음 에러 (404)
 */
class NotFoundError extends ApiError {
  constructor(message = '요청한 리소스를 찾을 수 없습니다.', resourceType = 'Resource', resourceId = null, errorCode = 'NOT_FOUND') {
    let fullMessage = message;
    if (resourceType && resourceId) {
      fullMessage = `${resourceType} with ID '${resourceId}' not found.`;
    } else if (resourceType) {
      fullMessage = `${resourceType} not found.`;
    }
    super(fullMessage, 404, errorCode, { resourceType, resourceId });
  }
}

/**
 * 외부 서비스 API 호출 실패 에러
 */
class ExternalServiceError extends AppError {
  constructor(serviceName, originalError, message = `외부 서비스(${serviceName}) 호출 중 오류가 발생했습니다.`, errorCode = 'EXTERNAL_SERVICE_FAILURE') {
    super(message, 502, errorCode, true, { // 502 Bad Gateway 또는 503 Service Unavailable 고려
      serviceName,
      originalErrorMessage: originalError?.message,
      originalErrorStatus: originalError?.response?.status,
      // originalErrorData: originalError?.response?.data, // 민감 정보 포함 가능성 주의
    });
    this.originalError = originalError; // 원본 에러 객체 참조
  }
}

/**
 * 작업 큐 관련 에러
 */
class JobQueueError extends AppError {
    constructor(queueName, jobDetails, originalError, message = `작업 큐(${queueName}) 처리 중 오류 발생.`, errorCode = 'JOB_QUEUE_ERROR') {
        super(message, 500, errorCode, true, {
            queueName,
            jobDetails, // 어떤 작업이었는지 식별 정보
            originalErrorMessage: originalError?.message,
        });
        this.originalError = originalError;
    }
}


module.exports = {
  AppError,
  ApiError,
  ValidationError,
  UnauthorizedError,
  ForbiddenError,
  NotFoundError,
  ExternalServiceError,
  JobQueueError,
};
