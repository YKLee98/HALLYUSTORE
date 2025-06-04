// src/jobs/workers/catalogWorker.js
const { Worker } = require('bullmq');
const config = require('../../config');
const logger = require('../../config/logger');
const { fetchAndProcessBunjangCatalog } = require('../../services/catalogService');
let catalogService;

async function initializeCatalogService() {
  if (!catalogService) {
    const module = await import('../services/catalogService');
    catalogService = module.default; // For default exports
    // catalogService = module; // For named exports
  }
  return catalogService;
}

// Then wrap your worker logic:
async function processCatalogJob(jobData) {
  const catalogService = await initializeCatalogService();
  // Use catalogService normally here
  await catalogService.syncProducts(jobData);
}
const { JobQueueError, AppError, ExternalServiceError } = require('../../utils/customErrors');

const CONCURRENCY = parseInt(process.env.CATALOG_WORKER_CONCURRENCY, 10) || config.worker?.catalogConcurrency || 1;

async function processCatalogJob(job) {
  // Use a distinct variable name for the job identifier string within this function
  // to ensure clarity and avoid potential scope confusion if 'jobIdForLog' was used elsewhere.
  const currentJobIdentifier = `Job ${job.id} (Name: ${job.name}, Trigger: ${job.data.triggeredBy || 'unknown'})`;
  const { catalogType } = job.data;

  logger.info(`[Worker: ${job.queueName}] Starting ${currentJobIdentifier}. Type: ${catalogType}`);

  if (!['full', 'segment'].includes(catalogType)) {
    logger.error(`[Worker: ${job.queueName}] ${currentJobIdentifier} has invalid catalogType: ${catalogType}`);
    // Throw a JobQueueError with the original error (null here as it's a validation error)
    throw new JobQueueError(job.queueName, job, null, `Invalid catalogType: ${catalogType} for ${currentJobIdentifier}`);
  }

  try {
    // Pass the well-defined 'currentJobIdentifier' to the service layer for consistent logging
    const resultSummary = await fetchAndProcessBunjangCatalog(catalogType, currentJobIdentifier);
    logger.info(`[Worker: ${job.queueName}] ${currentJobIdentifier} (Type: ${catalogType}) completed successfully. Summary:`, resultSummary);
    return { success: true, catalogType, summary: resultSummary };
  } catch (error) { // This 'error' is whatever was thrown from fetchAndProcessBunjangCatalog
    const originalErrorMessage = error.message || 'No original error message available';

    // Log detailed information about the error caught from the service layer
    logger.error(`[Worker: ${job.queueName}] ${currentJobIdentifier} (Type: ${catalogType}) FAILED. Caught Error: ${originalErrorMessage}`, {
      errorType: error.constructor.name,
      errorCode: error.code, // For AppError, ExternalServiceError
      serviceName: error.serviceName, // For ExternalServiceError
      details: error.details, // For AppError, ExternalServiceError
      originalStack: error.stack, // Stack trace of the caught error
      isOperational: error.isOperational, // For AppError
    });

    // Create a more informative message for BullMQ/Bull Board
    // This message will be the 'message' property of the JobQueueError
    const displayErrorMessage = `Catalog processing ${currentJobIdentifier}, Type: ${catalogType}, failed. Cause: ${originalErrorMessage.substring(0, 250)}`;
    
    // Wrap the caught error in JobQueueError to provide context to the job system
    // The original 'error' object is passed as the 'cause'
    throw new JobQueueError(job.queueName, job, error, displayErrorMessage);
  }
}

function createCatalogWorker(queueName, connection) {
  const worker = new Worker(queueName, processCatalogJob, {
    connection,
    concurrency: CONCURRENCY,
    lockDuration: config.worker?.catalogLockDurationMs || 30 * 60 * 1000, // 30 minutes
    // Other BullMQ worker options like 'attempts', 'backoff', etc., can be configured here
    // removeOnComplete: { count: 1000, age: 24 * 3600 },
    // removeOnFail: { count: 5000, age: 7 * 24 * 3600 },
  });

  worker.on('completed', (job, returnValue) => {
    logger.info(`[Worker: ${worker.name}] Job ${job.id} (Name: ${job.name}, Type: ${job.data.catalogType}) completed. Return: ${JSON.stringify(returnValue)}`);
  });

  worker.on('failed', (job, error) => { // 'error' here is the JobQueueError thrown from processCatalogJob
    logger.error(`[Worker: ${worker.name}] Job ${job?.id} (Name: ${job?.name}, Type: ${job?.data.catalogType}) failed ultimately after ${job?.attemptsMade} attempts. Error Message: ${error.message}`, {
        jobData: job?.data,
        jobId: job?.id,
        errorName: error.name, // Should be JobQueueError
        // error.cause will contain the original error object (e.g., ExternalServiceError)
        originalErrorName: error.cause?.name,
        originalErrorMessage: error.cause?.message,
        originalErrorCode: error.cause?.code,
        // Log a snippet of the original error's stack if available
        originalErrorStackSnippet: error.cause?.stack?.substring(0, 500) || "N/A",
    });
  });

  worker.on('error', (err) => {
    logger.error(`[Worker: ${worker.name}] General error in catalog worker instance (not specific to a job):`, err);
  });

  worker.on('stalled', (jobId, prev) => {
    logger.warn(`[Worker: ${worker.name}] Job ${jobId} stalled. Previous status: ${prev}. This job may be retried if attemptsMade < maxAttempts.`);
  });
  
  logger.info(`[Worker] Catalog Processing Worker listening on queue "${queueName}" with concurrency ${CONCURRENCY}. Lock duration: ${worker.opts.lockDuration / 1000}s.`);
  return worker;
}

module.exports = createCatalogWorker;
