// src/utils/validationHelper.js
// express-validator와 함께 사용될 유효성 검사 결과 처리 헬퍼입니다.

const { validationResult } = require('express-validator');
const { ValidationError } = require('./customErrors'); // 커스텀 에러 사용

/**
 * express-validator의 유효성 검사 결과를 확인하고,
 * 에러가 있으면 ValidationError를 throw합니다.
 * 이 함수는 라우트 핸들러에서 유효성 검사 규칙 실행 후 호출됩니다.
 *
 * @example
 * router.post(
 * '/user',
 * [ body('email').isEmail(), body('password').isLength({ min: 5 }) ],
 * handleValidationErrors, // 유효성 검사 규칙 뒤에 이 미들웨어 추가
 * userController.createUser
 * );
 */
function handleValidationErrors(req, res, next) {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    // 에러 배열을 좀 더 사용하기 쉬운 형태로 변환 (선택 사항)
    const extractedErrors = errors.array().map(err => ({
      field: err.type === 'field' ? err.path : (err.param || 'unknown_field'), // err.path (v7+) 또는 err.param
      message: err.msg,
      value: err.type === 'field' ? err.value : undefined, // err.value (v7+) 또는 err.value
    }));
    
    // ValidationError를 throw하거나, 직접 응답을 보낼 수 있습니다.
    // throw를 사용하면 중앙 에러 핸들러에서 처리됩니다.
    throw new ValidationError('입력값 유효성 검사에 실패했습니다.', extractedErrors);
    
    // 또는 직접 응답:
    // return res.status(422).json({
    //   error: 'Validation Failed',
    //   messages: extractedErrors,
    // });
  }
  // 유효성 검사 통과 시 다음 미들웨어/핸들러로 진행
  next();
}

module.exports = {
  handleValidationErrors,
};
