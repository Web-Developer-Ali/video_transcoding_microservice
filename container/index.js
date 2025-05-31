import { 
  updateTranscodingStatus, 
  shutdownDatabase, 
  updateMasterPlaylistInDatabase, 
  insertFileMetadata,
  ensureDatabaseConnection
} from './database.js';
import { downloadBlob, uploadBlobs, deleteBlob } from './storage.js';
import { transcodeVideoForAdaptiveStreaming, cleanupFiles, getVideoDuration } from './transcoder.js';
import path from 'path';
import sanitize from 'sanitize-filename';
import winston from 'winston';
import fs from 'fs/promises';
import os from 'os';
import express from 'express';

// Health check server
const app = express();
app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'healthy',
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    disk: {
      tmpdir: os.tmpdir(),
      freemem: os.freemem() / (1024 * 1024) + 'MB'
    }
  });
});
const healthServer = app.listen(8080);

// Configure structured logging
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [
    new winston.transports.Console({
      format: winston.format.printf(({ timestamp, level, message, stack }) => 
        `${timestamp} [${level.toUpperCase()}]: ${message}${stack ? `\n${stack}` : ''}`)
    })
  ],
  exitOnError: false
});

// Constants
const MAX_FILE_SIZE_MB = 200;
const PROCESS_TIMEOUT_MS = 3600 * 1000; // 1 hour
const MIN_DISK_SPACE_MB = 500;

// Validate all required environment variables
function validateEnvironment() {
  const requiredEnvVars = [
    'MESSAGE_CONTENT',
    'chapterId',
    'INPUT_CONTAINER_NAME',
    'OUTPUT_CONTAINER_NAME',
    'AZURE_STORAGE_CONNECTION_STRING',
    'AZURE_STORAGE_ACCOUNT_NAME',
    'NEON_POSTGRES_URL',
    'CONCURRENCY_LIMIT'
  ];

  const missingVars = requiredEnvVars.filter(env => !process.env[env]);
  
  if (missingVars.length > 0) {
    const error = new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
    error.code = 'ENV_VAR_MISSING';
    throw error;
  }
}

// Check system resources
async function checkSystemResources() {
  const stats = await fs.statfs(os.tmpdir());
  const freeSpaceMB = (stats.bsize * stats.bfree) / (1024 * 1024);
  
  if (freeSpaceMB < MIN_DISK_SPACE_MB) {
    throw new Error(`Insufficient disk space. Required: ${MIN_DISK_SPACE_MB}MB, Available: ${freeSpaceMB}MB`);
  }

  const memory = process.memoryUsage();
  if (memory.rss > 800 * 1024 * 1024) { // 800MB
    logger.warn('High memory usage detected', { memory });
  }
}

// Main processing function with comprehensive error handling
async function processTask() {
  let videoPath;
  let outputFiles = [];
  let chapterId;
  let blobName;
  let timeout;

  try {
    // Phase 1: Initialization and validation
    validateEnvironment();
    await checkSystemResources();
    await ensureDatabaseConnection();
    
    // Set processing timeout
    timeout = setTimeout(() => {
      throw new Error(`Processing timeout after ${PROCESS_TIMEOUT_MS/1000} seconds`);
    }, PROCESS_TIMEOUT_MS);

    chapterId = process.env.chapterId;
    if (!chapterId || isNaN(chapterId)) {
      throw new Error(`Invalid chapterId: ${chapterId}`);
    }

    logger.info(`Starting adaptive streaming process for ChapterID: ${chapterId}`, { 
      chapterId,
      system: {
        memory: process.memoryUsage(),
        cpus: os.cpus().length,
        freemem: os.freemem() / (1024 * 1024) + 'MB'
      }
    });

    // Phase 2: Message parsing
    let message;
    try {
      const messageContent = Buffer.from(process.env.MESSAGE_CONTENT, "base64").toString("utf-8");
      message = JSON.parse(messageContent);
      
      if (!message?.data?.url) {
        throw new Error("Message missing 'data.url' property");
      }
      
      blobName = sanitize(path.basename(message.data.url));
      if (!blobName) {
        throw new Error("Could not extract valid blob name from URL");
      }
    } catch (error) {
      error.code = error.code || 'MESSAGE_PARSING_ERROR';
      throw error;
    }

    logger.info(`Processing blob: ${blobName}`, { blobName });

    // Phase 3: Update status to Processing
    await updateTranscodingStatus(chapterId, 'Processing');

    // Phase 4: Download original video with size validation
    try {
      videoPath = await downloadBlob(blobName, process.env.INPUT_CONTAINER_NAME);
      const stats = await fs.stat(videoPath);
      
      if (stats.size === 0) {
        throw new Error("Downloaded file is empty");
      }

      const fileSizeMB = stats.size / (1024 * 1024);
      if (fileSizeMB > MAX_FILE_SIZE_MB) {
        throw new Error(`File size ${fileSizeMB.toFixed(2)}MB exceeds maximum allowed ${MAX_FILE_SIZE_MB}MB`);
      }
      
      logger.info(`Video downloaded successfully (${stats.size} bytes)`, { 
        path: videoPath,
        size: stats.size,
        sizeMB: fileSizeMB.toFixed(2)
      });

      // Check video duration
      const duration = await getVideoDuration(videoPath);
      logger.info(`Video duration: ${duration} seconds`);
      if (duration > 3600) { // 1 hour
        logger.warn('Long video duration detected', { duration });
      }
    } catch (error) {
      error.code = error.code || 'DOWNLOAD_FAILED';
      throw error;
    }

    // Phase 5: Transcoding
    try {
      outputFiles = await transcodeVideoForAdaptiveStreaming(videoPath, blobName, chapterId);
      logger.info(`Transcoding completed. Generated ${outputFiles.length} files`, {
        fileCount: outputFiles.length,
        files: outputFiles.map(f => path.basename(f))
      });
    } catch (error) {
      error.code = error.code || 'TRANSCODING_FAILED';
      throw error;
    }

    // Phase 6: Upload results
    let uploadResult;
    try {
      uploadResult = await uploadBlobs(
        outputFiles, 
        process.env.OUTPUT_CONTAINER_NAME, 
        chapterId,
        process.env.SKIP_METADATA ? null : insertFileMetadata
      );

      if (!uploadResult?.masterPlaylistUrl) {
        throw new Error('Master playlist URL not generated');
      }

      logger.info(`Upload completed. Master playlist URL: ${uploadResult.masterPlaylistUrl}`, {
        masterPlaylistUrl: uploadResult.masterPlaylistUrl,
        virtualFolder: uploadResult.virtualFolder
      });
    } catch (error) {
      error.code = error.code || 'UPLOAD_FAILED';
      throw error;
    }

    // Phase 7: Database updates
    try {
      // Store master playlist in Courses_Chapters table
      await updateMasterPlaylistInDatabase(chapterId, uploadResult.masterPlaylistUrl);
      
      // Store all files in ChapterFiles table if metadata is enabled
      if (!process.env.SKIP_METADATA) {
        const filesToStore = outputFiles.map(file => ({
          name: `${uploadResult.virtualFolder}${path.basename(file)}`,
          container: process.env.OUTPUT_CONTAINER_NAME
        }));
        
        await Promise.all(
          filesToStore.map(file => 
            insertFileMetadata(chapterId, file.name, file.container)
              .catch(err => {
                logger.warn(`Failed to record metadata for ${file.name}`, {
                  error: err.message,
                  chapterId,
                  blobName: file.name
                });
              })
          )
        );
      }
      
      logger.info(`Database updates completed for ChapterID: ${chapterId}`);
    } catch (error) {
      error.code = error.code || 'DATABASE_UPDATE_FAILED';
      throw error;
    }

    // Phase 8: Cleanup
    try {
      const cleanupFilesList = [videoPath, ...outputFiles];
      await cleanupFiles(cleanupFilesList);
      
      await deleteBlob(blobName, process.env.INPUT_CONTAINER_NAME)
        .catch(err => logger.warn(`Input blob deletion warning: ${err.message}`));
      
      logger.info('Cleanup completed', { 
        deletedFiles: cleanupFilesList.length 
      });
    } catch (error) {
      logger.warn(`Cleanup warnings: ${error.message}`, { 
        error: error.stack 
      });
    }

    // Final status update
    await updateTranscodingStatus(chapterId, 'Completed');
    logger.info(`Processing completed successfully for ChapterID: ${chapterId}`, {
      chapterId,
      duration: process.uptime(),
      memoryUsage: process.memoryUsage()
    });

  } catch (error) {
    // Comprehensive error handling
    logger.error(`Process failed: ${error.message}`, { 
      error: {
        message: error.message,
        stack: error.stack,
        code: error.code || 'UNKNOWN_ERROR'
      },
      chapterId,
      blobName,
      system: {
        memory: process.memoryUsage(),
        loadavg: os.loadavg()
      }
    });
    
    // Update status to Failed if we have a chapterId
    if (chapterId) {
      await updateTranscodingStatus(chapterId, 'Failed', error.message)
        .catch(dbError => 
          logger.error('Failed to update transcoding status', {
            originalError: error.message,
            dbError: dbError.message
          })
        );
    }

    // Attempt cleanup on failure
    try {
      if (videoPath) await fs.unlink(videoPath).catch(() => {});
      if (outputFiles.length > 0) await cleanupFiles(outputFiles).catch(() => {});
    } catch (cleanupError) {
      logger.warn('Cleanup during failure encountered errors', {
        error: cleanupError.message
      });
    }

    throw error;
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

// Graceful shutdown handler
async function handleShutdown(signal) {
  logger.info(`Received ${signal}, shutting down gracefully...`, { 
    signal,
    uptime: process.uptime()
  });
  try {
    // Close health check server first
    await new Promise(resolve => healthServer.close(resolve));
    
    // Then shutdown database
    await shutdownDatabase();
    logger.info('Shutdown completed successfully');
    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown:', { 
      error: {
        message: error.message,
        stack: error.stack
      }
    });
    process.exit(1);
  }
}

// Process lifecycle management
process.on("SIGTERM", () => handleShutdown('SIGTERM'));
process.on("SIGINT", () => handleShutdown('SIGINT'));
process.on("unhandledRejection", (reason, promise) => {
  logger.error('Unhandled Rejection at:', { 
    promise, 
    reason: reason instanceof Error ? reason.stack : reason 
  });
});
process.on("uncaughtException", error => {
  logger.error('Uncaught Exception:', { error: error.stack });
  handleShutdown('UNCAUGHT_EXCEPTION');
});

// Resource monitoring
setInterval(() => {
  logger.debug('Resource snapshot', {
    memory: process.memoryUsage(),
    cpu: os.loadavg(),
    disk: {
      tmpdir: os.tmpdir(),
      freemem: os.freemem() / (1024 * 1024) + 'MB'
    }
  });
}, 60000); // Every minute

// Entry point
processTask()
  .then(() => process.exit(0))
  .catch(error => {
    logger.error('Fatal error during process execution', { 
      error: {
        message: error.message,
        stack: error.stack,
        code: error.code || 'FATAL_ERROR'
      },
      system: {
        memory: process.memoryUsage(),
        uptime: process.uptime()
      }
    });
    process.exit(1);
  });