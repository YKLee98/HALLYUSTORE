// src/utils/jwtHelper.js
// 번개장터 API 인증을 위한 JWT 생성 유틸리티입니다.

const jwt = require('jsonwebtoken');
const { v4: uuidv4 } = require('uuid');
const config = require('../config');
const logger = require('../config/logger');
const { AppError } = require('./customErrors');

/**
 * 번개장터 API 요청을 위한 JWT를 생성합니다.
 * 토큰 유효기간은 매우 짧으므로 (API 문서 기준 5초), 각 요청 직전에 생성합니다.
 * @param {boolean} [includeNonce=true] - POST, PUT, DELETE 요청 시 true. GET 요청 시에도 nonce를 포함하는 것이 번개장터 샘플 코드 기준.
 * @returns {string} 생성된 JWT (Bearer 접두어 제외).
 * @throws {AppError} JWT 생성 실패 시 (예: 설정 누락).
 */
function generateBunjangToken(includeNonce = true) {
  try {
    const { accessKey, secretKey } = config.bunjang;
    const { expirationSeconds } = config.jwt;

    if (!accessKey || !secretKey) {
      logger.error('Bunjang API accessKey or secretKey is missing in configuration.');
      throw new AppError('번개장터 API 인증 정보가 설정되지 않았습니다.', 500, 'BUNJANG_CREDENTIALS_MISSING');
    }

    const secretKeyBuffer = Buffer.from(secretKey, 'base64');
    const issuedAt = Math.floor(Date.now() / 1000); // 현재 시간 (초 단위)

    const payload = {
      iat: issuedAt,
      accessKey: accessKey,
    };

    if (includeNonce) {
      payload.nonce = uuidv4();
    }
    
    const signOptions = {
      algorithm: 'HS256',
      // expiresIn: `${expirationSeconds}s`, // 토큰 자체 만료 시간. 번개장터는 iat + 5초로 검증.
    };

    const token = jwt.sign(payload, secretKeyBuffer, signOptions);
    // logger.debug('Generated Bunjang JWT.', { iat: payload.iat, nonce: payload.nonce, exp: issuedAt + 5 }); // 5초 후 만료 예상
    return token;

  } catch (error) {
    logger.error('Failed to generate Bunjang JWT:', {
      message: error.message,
      // stack: error.stack, // 상세 스택은 필요시 로깅
    });
    // AppError로 래핑하여 일관된 에러 처리
    if (error instanceof AppError) throw error;
    throw new AppError(`번개장터 JWT 생성 실패: ${error.message}`, 500, 'JWT_GENERATION_FAILED');
  }
}

module.exports = {
  generateBunjangToken,
};
