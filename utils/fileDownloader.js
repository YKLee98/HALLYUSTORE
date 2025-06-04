// src/utils/fileDownloader.js
// 파일 다운로드 및 Gzip 압축 해제 유틸리티. fs-extra 및 스트림 파이프라인 사용.

const axios = require('axios');
const zlib = require('zlib');
const fs = require('fs-extra'); // fs-extra는 mkdirp, rimraf 등 편리한 기능 제공
const path = require('path');
const { promisify } = require('util');
const streamPipeline = promisify(require('stream').pipeline);

const logger = require('../config/logger');
const { generateBunjangToken } = require('./jwtHelper');
const config = require('../config');
const { ExternalServiceError, AppError } = require('./customErrors');

/**
 * 주어진 URL에서 파일을 스트림으로 다운로드하고 지정된 경로에 저장합니다.
 * @param {string} fileUrl - 다운로드할 파일의 전체 URL.
 * @param {string} outputPath - 파일 저장 경로 (파일명 포함).
 * @param {boolean} [requiresAuth=true] - 번개장터 인증 토큰 필요 여부.
 * @param {number} [timeout=config.bunjang.catalogDownloadTimeoutMs] - 요청 타임아웃 (ms).
 * @returns {Promise<string>} 성공 시 저장된 파일의 절대 경로.
 * @throws {ExternalServiceError|AppError} 다운로드 또는 파일 저장 실패 시.
 */
async function downloadFile(fileUrl, outputPath, requiresAuth = true, timeout = config.bunjang.catalogDownloadTimeoutMs) {
  const absoluteOutputPath = path.resolve(outputPath); // 절대 경로로 변환
  logger.info(`[FileDownloader] Attempting to download file: ${fileUrl} -> ${absoluteOutputPath}`);

  const headers = {
    'Accept-Encoding': 'gzip, deflate, br', // 서버가 지원하는 압축 방식 명시
    'User-Agent': `${config.appName}/${config.version} (FileDownloader)`, // User-Agent 설정
  };
  if (requiresAuth) {
    try {
      const token = generateBunjangToken(true);
      headers['Authorization'] = `Bearer ${token}`;
    } catch (jwtError) { // AppError
      logger.error(`[FileDownloader] JWT generation failed for URL ${fileUrl}: ${jwtError.message}`);
      throw jwtError;
    }
  }

  const outputDir = path.dirname(absoluteOutputPath);
  try {
    await fs.ensureDir(outputDir);
  } catch (dirError) {
    logger.error(`[FileDownloader] Failed to ensure output directory ${outputDir}:`, dirError);
    throw new AppError(`출력 디렉토리 생성 실패: ${outputDir}`, 500, 'FILE_SYSTEM_ERROR_DIR', true, { path: outputDir });
  }
  
  let responseStream;
  try {
    const response = await axios({
      method: 'GET',
      url: fileUrl,
      responseType: 'stream',
      headers: headers,
      timeout: timeout,
    });

    // Axios는 2xx 이외의 상태 코드에 대해 에러를 throw하므로, 여기서는 response.status가 2xx라고 가정.
    responseStream = response.data;

    const writer = fs.createWriteStream(absoluteOutputPath);
    await streamPipeline(responseStream, writer);

    logger.info(`[FileDownloader] File downloaded successfully: ${absoluteOutputPath}`);
    return absoluteOutputPath;

  } catch (error) {
    // 다운로드 실패 시 임시 파일 삭제 시도
    try { if (await fs.pathExists(absoluteOutputPath)) await fs.remove(absoluteOutputPath); } catch (e) { logger.warn(`[FileDownloader] Failed to remove partially downloaded file ${absoluteOutputPath}:`, e); }

    if (axios.isAxiosError(error)) {
      const errDetails = {
        message: error.message, code: error.code, status: error.response?.status,
        requestUrl: error.config?.url,
        // responseData: error.response?.data ? (typeof error.response.data.pipe === 'function' ? '[Stream]' : JSON.stringify(error.response.data).substring(0,200)) : undefined,
      };
      logger.error(`[FileDownloader] Axios error downloading file from ${fileUrl}:`, errDetails);
      throw new ExternalServiceError('BunjangFileDownload', error, `파일 다운로드 중 Axios 오류 (URL: ${fileUrl})`, 'BUNJANG_DOWNLOAD_AXIOS_ERROR', errDetails);
    }
    logger.error(`[FileDownloader] Unexpected error downloading file from ${fileUrl}:`, error);
    if (error instanceof AppError || error instanceof ExternalServiceError) throw error;
    throw new AppError(`파일 다운로드 중 예기치 않은 오류 발생: ${error.message}`, 500, 'FILE_DOWNLOAD_UNEXPECTED_ERROR');
  }
}

/**
 * Gzip으로 압축된 파일을 해제합니다.
 * @param {string} gzippedFilePath - 압축된 파일의 경로.
 * @param {string} outputFilePath - 압축 해제된 파일 저장 경로.
 * @returns {Promise<string>} 성공 시 압축 해제된 파일의 절대 경로.
 * @throws {AppError} 압축 해제 실패 시.
 */
async function unzipGzFile(gzippedFilePath, outputFilePath) {
  const absGzippedPath = path.resolve(gzippedFilePath);
  const absOutputPath = path.resolve(outputFilePath);
  logger.info(`[FileDownloader] Attempting to unzip: ${absGzippedPath} -> ${absOutputPath}`);
  try {
    if (!await fs.pathExists(absGzippedPath)) {
        throw new AppError(`압축 해제할 파일이 없습니다: ${absGzippedPath}`, 404, 'FILE_NOT_FOUND_FOR_UNZIP', true, { path: absGzippedPath });
    }
    const outputDir = path.dirname(absOutputPath);
    await fs.ensureDir(outputDir); // 출력 디렉토리 확인 및 생성

    const reader = fs.createReadStream(absGzippedPath);
    const gunzip = zlib.createGunzip();
    const writer = fs.createWriteStream(absOutputPath);

    await streamPipeline(reader, gunzip, writer);
    logger.info(`[FileDownloader] File unzipped successfully: ${absOutputPath}`);
    return absOutputPath;
  } catch (error) {
    logger.error(`[FileDownloader] Error unzipping file ${absGzippedPath}:`, error);
    try { if (await fs.pathExists(absOutputPath)) await fs.remove(absOutputPath); } catch (e) { logger.warn(`[FileDownloader] Failed to remove partially unzipped file ${absOutputPath}:`, e); }
    if (error instanceof AppError) throw error;
    throw new AppError(`Gzip 파일 압축 해제 실패 (${path.basename(absGzippedPath)}): ${error.message}`, 500, 'FILE_UNZIP_ERROR');
  }
}

module.exports = {
  downloadFile,
  unzipGzFile,
};
