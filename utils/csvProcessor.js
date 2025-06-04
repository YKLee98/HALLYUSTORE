// src/utils/csvProcessor.js
// CSV 파일 파싱 유틸리티. fs-extra 및 스트림 기반으로 대용량 파일 처리.

const fs = require('fs-extra'); // fs-extra는 mkdirp, pathExists 등 편리한 기능 제공
const csv = require('csv-parser');
const logger = require('../config/logger'); // ../config/logger.js
const { AppError } = require('./customErrors'); // ./customErrors.js

/**
 * CSV 파일을 읽어 객체 배열로 파싱합니다. 스트림 기반으로 대용량 파일 처리 가능.
 * @param {string} filePath - CSV 파일 경로.
 * @param {function} [rowProcessorAsync] - (선택 사항) 각 행을 비동기적으로 처리하는 함수.
 * 행 객체와 행 번호를 인자로 받고, Promise<object|null|undefined>를 반환해야 합니다.
 * null/undefined 반환 시 해당 행은 결과에서 제외됩니다.
 * @param {object} [csvParserOptions] - csv-parser 라이브러리 옵션.
 * @returns {Promise<Array<object>>} 파싱 및 처리된 데이터 객체의 배열.
 * @throws {AppError} 파일 읽기 또는 파싱 실패 시.
 */
async function parseCsvFile(filePath, rowProcessorAsync = null, csvParserOptions = {}) {
  const absoluteFilePath = require('path').resolve(filePath); // 경로를 절대 경로로 변환 (로깅용)
  logger.info(`[CsvProcessor] Starting to parse CSV file: ${absoluteFilePath}`);
  const results = [];
  let rowCount = 0;
  let processedAndIncludedRowCount = 0;

  if (!await fs.pathExists(absoluteFilePath)) {
    logger.error(`[CsvProcessor] CSV file not found: ${absoluteFilePath}`);
    throw new AppError(`CSV 파일을 찾을 수 없습니다: ${absoluteFilePath}`, 404, 'CSV_FILE_NOT_FOUND', true, { path: absoluteFilePath });
  }

  return new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream(absoluteFilePath);
    
    fileStream.on('error', (streamError) => { // 파일 스트림 자체의 에러 처리
        logger.error(`[CsvProcessor] Error reading file stream for ${absoluteFilePath}:`, streamError);
        reject(new AppError(`CSV 파일 스트림 오류: ${streamError.message}`, 500, 'CSV_STREAM_ERROR', true, { path: absoluteFilePath }));
    });

    const csvStream = csv({
      mapHeaders: ({ header }) => header.trim(), // 헤더 공백 제거
      skipEmptyLines: true, // 빈 줄 무시
      ...csvParserOptions, // 사용자 정의 옵션 (예: separator, quote 등)
    });

    const processingPipeline = fileStream.pipe(csvStream);

    processingPipeline.on('data', async (data) => {
      processingPipeline.pause(); // 백프레셔: rowProcessorAsync 작업이 끝날 때까지 스트림 일시 중지
      rowCount++;
      try {
        let processedData = data;
        if (rowProcessorAsync && typeof rowProcessorAsync === 'function') {
          processedData = await rowProcessorAsync(data, rowCount); // 비동기 처리 함수 호출
        }

        if (processedData !== null && typeof processedData !== 'undefined') {
          results.push(processedData);
          processedAndIncludedRowCount++;
        }
      } catch (rowError) {
        logger.warn(`[CsvProcessor] Error processing CSV row #${rowCount} in file ${absoluteFilePath}:`, {
          errorMessage: rowError.message,
          // rowData: JSON.stringify(data).substring(0, 200), // 너무 길 수 있으므로 요약 또는 필요시 전체
        });
        // 특정 행 처리 실패 시 전체 파싱을 중단할지, 아니면 계속할지 결정.
        // 현재는 로깅만 하고 계속 진행.
        // 만약 중단하려면:
        // processingPipeline.destroy(new AppError(`CSV 행 처리 중 오류 (행 #${rowCount}): ${rowError.message}`, 500, 'CSV_ROW_PROCESSING_ERROR'));
        // return; // stream.destroy() 호출 후에는 resume()하면 안됨.
      } finally {
        if (!processingPipeline.destroyed) { // 스트림이 destroy되지 않았을 때만 resume
            processingPipeline.resume(); // 다음 데이터 처리를 위해 스트림 재개
        }
      }
    });

    processingPipeline.on('end', () => {
      logger.info(`[CsvProcessor] CSV file parsed successfully: ${absoluteFilePath}. Total rows read: ${rowCount}, Processed and included rows: ${processedAndIncludedRowCount}`);
      resolve(results);
    });

    processingPipeline.on('error', (error) => { // csv-parser 또는 파이프라인 에러
      logger.error(`[CsvProcessor] Error parsing CSV file ${absoluteFilePath} or in pipeline:`, error);
      // AppError가 아닌 경우 래핑
      if (error instanceof AppError) return reject(error);
      reject(new AppError(`CSV 파일 파싱 또는 파이프라인 오류: ${error.message}`, 500, 'CSV_PARSING_PIPELINE_ERROR', true, { originalError: error }));
    });
  });
}

module.exports = {
  parseCsvFile,
};
