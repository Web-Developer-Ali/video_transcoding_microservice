// Import necessary modules
import { BlobServiceClient } from "@azure/storage-blob";
import ffmpeg from "fluent-ffmpeg";
import fs from "fs/promises";
import path from "path";
import os from "os";
import pLimit from "p-limit";
import sanitize from "sanitize-filename";
import winston from "winston";
import mime from "mime-types";
import sql from "mssql";

// Logging setup
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => `${timestamp} [${level}]: ${message}`)
  ),
  transports: [new winston.transports.Console()],
});

// Environment Variables
const requiredEnvVars = [
  "AZURE_STORAGE_CONNECTION_STRING",
  "MESSAGE_CONTENT",
  "AZURE_SQL_USER",
  "AZURE_SQL_PASSWORD",
  "AZURE_SQL_SERVER",
  "AZURE_SQL_DATABASE",
  "CONCURRENCY_LIMIT",
  "INPUT_CONTAINER_NAME",
  "OUTPUT_CONTAINER_NAME",
  "chapterId",
];
requiredEnvVars.forEach((env) => {
  if (!process.env[env]) throw new Error(`${env} is missing`);
});

const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
const tempDir = os.tmpdir(); // Cross-platform temporary directory
const containerNameInput = process.env.INPUT_CONTAINER_NAME || "chaperters-videos-temp";
const containerNameOutput = process.env.OUTPUT_CONTAINER_NAME || "chaperters-videos-transcoded";
const concurrencyLimit = parseInt(process.env.CONCURRENCY_LIMIT) || 2;

// Database configuration
const sqlConfig = {
  user: process.env.AZURE_SQL_USER,
  password: process.env.AZURE_SQL_PASSWORD,
  server: process.env.AZURE_SQL_SERVER,
  database: process.env.AZURE_SQL_DATABASE,
  options: {
    encrypt: true,
    trustServerCertificate: true,
  },
};

// Helper function for creating directories
const ensureDirectoryExists = async (dirPath) => {
  try {
    await fs.mkdir(dirPath, { recursive: true });
  } catch (error) {
    logger.error(`Failed to ensure directory exists: ${dirPath}`, error);
    throw error;
  }
};

// Reusable database connection function
const withDatabaseConnection = async (callback) => {
  let pool;
  try {
    pool = await sql.connect(sqlConfig);
    return await callback(pool);
  } catch (error) {
    logger.error("Database connection error:", error);
    throw new Error("Failed to connect to the database");
  } finally {
    if (pool) await pool.close();
  }
};

// Update video field in database
const updateVideoFieldInDatabase = async (chapterId, videoUrl) => {
  return withDatabaseConnection(async () => {
    const request = new sql.Request();
    const query = `
      UPDATE Courses_Chapters
      SET Video = @videoUrl,
          UpdatedAt = GETDATE()
      WHERE ChapterID = @chapterId
    `;
    request.input('videoUrl', sql.NVarChar, videoUrl);
    request.input('chapterId', sql.Int, parseInt(chapterId));
    await request.query(query);
    logger.info(`Video URL and status updated for ChapterID: ${chapterId}`);
  });
};

// Update transcoding error in database
const updateTranscodingError = async (chapterId, errorMessage) => {
  return withDatabaseConnection(async () => {
    const request = new sql.Request();
    const query = `
      UPDATE Courses_Chapters
      SET TranscodingStatus = 'Failed',
          TranscodingError = @errorMessage,
          UpdatedAt = GETDATE()
      WHERE ChapterID = @chapterId
    `;
    request.input('errorMessage', sql.NVarChar, errorMessage);
    request.input('chapterId', sql.Int, parseInt(chapterId));
    await request.query(query);
    logger.info(`Error status updated for ChapterID: ${chapterId}`);
  });
};

// Insert file metadata into ChapterFiles table
const insertFileMetadata = async (chapterId, blobName, containerName) => {
  return withDatabaseConnection(async () => {
    const request = new sql.Request();
    const query = `
      INSERT INTO ChapterFiles (ChapterID, BlobName, ContainerName)
      VALUES (@chapterId, @blobName, @containerName)
    `;
    request.input('chapterId', sql.Int, chapterId);
    request.input('blobName', sql.NVarChar, blobName);
    request.input('containerName', sql.NVarChar, containerName);
    await request.query(query);
    logger.info(`File metadata inserted for ChapterID: ${chapterId}, BlobName: ${blobName}`);
  });
};

// Download blob from Azure Storage
const downloadBlob = async (blobName, containerName) => {
  const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
  const containerClient = blobServiceClient.getContainerClient(containerName);
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);
  const downloadPath = path.join(tempDir, blobName);

  logger.info(`Downloading blob: ${blobName} from container: ${containerName}`);
  try {
    await blockBlobClient.downloadToFile(downloadPath);
    logger.info(`Blob downloaded to: ${downloadPath}`);
    return downloadPath;
  } catch (error) {
    logger.error(`Error downloading blob ${blobName}:`, error);
    throw new Error(`Failed to download blob: ${blobName}`);
  }
};

// Transcode video for adaptive streaming
const transcodeVideoForAdaptiveStreaming = async (inputPath, fileName) => {
  const resolutions = {
    "240p": "426x240",
    "360p": "640x360",
    "720p": "1280x720",
    "1080p": "1920x1080",
  };
  const outputPaths = [];
  const masterPlaylistPath = path.join(tempDir, `${fileName.split(".")[0]}_master.m3u8`);
  const limit = pLimit(concurrencyLimit); // Configurable concurrency limit

  logger.info("Starting video transcoding for adaptive streaming...");
  try {
    const transcodePromises = Object.entries(resolutions).map(([resolution, size]) =>
      limit(async () => {
        const outputFolder = path.join(tempDir, resolution);
        await ensureDirectoryExists(outputFolder);

        const resolutionPlaylist = path.join(outputFolder, `${fileName.split(".")[0]}_${resolution}.m3u8`);
        return new Promise((resolve, reject) => {
          ffmpeg(inputPath)
            .outputOptions([
              `-vf scale=${size}`,
              "-preset veryfast",
              "-g 48",
              "-sc_threshold 0",
              "-hls_time 4", // 4-second segments
              "-hls_playlist_type vod",
              `-hls_segment_filename ${outputFolder}/${fileName.split(".")[0]}_${resolution}_%03d.ts`,
            ])
            .output(resolutionPlaylist)
            .on("end", () => {
              logger.info(`Transcoding for ${resolution} completed.`);
              outputPaths.push(resolutionPlaylist);
              resolve();
            })
            .on("error", (err) => {
              logger.error(`Error transcoding to ${resolution}:`, err);
              reject(new Error(`Failed to transcode to ${resolution}`));
            })
            .run();
        });
      })
    );

    await Promise.all(transcodePromises);

    // Generate master playlist
    const masterPlaylistContent = Object.keys(resolutions)
      .map(
        (res) =>
          `#EXT-X-STREAM-INF:BANDWIDTH=${getBandwidth(res)},RESOLUTION=${resolutions[res]}\n${res}/${fileName.split(".")[0]}_${res}.m3u8`
      )
      .join("\n");
    await fs.writeFile(masterPlaylistPath, masterPlaylistContent);
    outputPaths.push(masterPlaylistPath);

    logger.info("All resolutions transcoded successfully.");
    return outputPaths;
  } catch (error) {
    logger.error("Error during transcoding:", error);
    throw new Error("Failed to transcode video for adaptive streaming");
  }
};

// Upload blobs to Azure Storage and insert metadata
const uploadBlobs = async (filePaths, containerName, chapterId) => {
  const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
  const containerClient = blobServiceClient.getContainerClient(containerName);
  let masterPlaylistUrl = '';

  logger.info(`Uploading files to container: ${containerName}`);
  try {
    for (const filePath of filePaths) {
      const fileName = path.basename(filePath);
      const timestamp = Date.now();
      const uniqueBlobName = `${timestamp}_${fileName}`; // Unique blob name

      const blockBlobClient = containerClient.getBlockBlobClient(uniqueBlobName);
      const contentType = mime.lookup(fileName) || "application/octet-stream";

      // Upload the file to Azure Storage
      await blockBlobClient.uploadFile(filePath, {
        blobHTTPHeaders: { blobContentType: contentType },
      });

      // Insert the file metadata into the database
      await insertFileMetadata(chapterId, uniqueBlobName, containerName);

      if (fileName.includes('_master.m3u8')) {
        masterPlaylistUrl = blockBlobClient.url;
        logger.info(`Master playlist URL: ${masterPlaylistUrl}`);
      }

      logger.info(`Uploaded: ${uniqueBlobName} with Content-Type: ${contentType}`);
    }

    if (masterPlaylistUrl) {
      await updateVideoFieldInDatabase(chapterId, masterPlaylistUrl);
      logger.info(`Database updated with master playlist URL for chapter ${chapterId}`);
    }

    return masterPlaylistUrl;
  } catch (error) {
    logger.error(`Error uploading blobs:`, error);
    throw new Error("Failed to upload blobs");
  }
};

// Clean up temporary files
const cleanupFiles = async (filePaths) => {
  for (const filePath of filePaths) {
    try {
      await fs.unlink(filePath);
      logger.info(`Deleted temporary file: ${filePath}`);
    } catch (error) {
      logger.error(`Failed to delete file: ${filePath}`, error);
    }
  }
};

// Get bandwidth for a resolution
const getBandwidth = (resolution) => {
  const bandwidthMap = {
    "240p": 800000,
    "360p": 1500000,
    "720p": 3000000,
    "1080p": 6000000,
  };
  return bandwidthMap[resolution] || 1000000;
};

// Delete blob from Azure Storage
const deleteBlob = async (blobName, containerName) => {
  const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
  const containerClient = blobServiceClient.getContainerClient(containerName);
  const blockBlobClient = containerClient.getBlockBlobClient(blobName);

  logger.info(`Deleting blob: ${blobName} from container: ${containerName}`);
  try {
    await blockBlobClient.delete();
    logger.info(`Blob deleted: ${blobName}`);
  } catch (error) {
    logger.error(`Error deleting blob ${blobName}:`, error);
    throw new Error(`Failed to delete blob: ${blobName}`);
  }
};

// Process task
const processTask = async () => {
  try {
    logger.info("Starting Adaptive Streaming Process...");

    const messageContentBase64 = process.env.MESSAGE_CONTENT;
    const messageContent = Buffer.from(messageContentBase64, "base64").toString("utf-8");
    const message = JSON.parse(messageContent);
    if (!message.subject) throw new Error("Invalid message content: subject is missing");

    const blobName = sanitize(path.basename(message.subject));
    const chapterId = process.env.chapterId;

    // Set TranscodingStatus to 'Processing'
    await withDatabaseConnection(async () => {
      const request = new sql.Request();
      const query = `
        UPDATE Courses_Chapters
        SET TranscodingStatus = 'Processing',
            UpdatedAt = GETDATE()
        WHERE ChapterID = @chapterId
      `;
      request.input('chapterId', sql.Int, parseInt(chapterId));
      await request.query(query);
      logger.info(`TranscodingStatus set to 'Processing' for ChapterID: ${chapterId}`);
    });

    const videoPath = await downloadBlob(blobName, containerNameInput);
    logger.info(`Video downloaded to path: ${videoPath}`);

    const outputFiles = await transcodeVideoForAdaptiveStreaming(videoPath, blobName);
    logger.info("Transcoding completed for all resolutions.");

    await uploadBlobs(outputFiles, containerNameOutput, chapterId);
    logger.info("Files uploaded successfully.");

    await cleanupFiles([videoPath, ...outputFiles]);
    logger.info("Temporary files cleaned up.");

    // Delete the original blob from the temporary container
    await deleteBlob(blobName, containerNameInput);
    logger.info(`Blob deleted from temporary container: ${blobName}`);

    // Set TranscodingStatus to 'Completed'
    await withDatabaseConnection(async () => {
      const request = new sql.Request();
      const query = `
        UPDATE Courses_Chapters
        SET TranscodingStatus = 'Completed',
            UpdatedAt = GETDATE()
        WHERE ChapterID = @chapterId
      `;
      request.input('chapterId', sql.Int, parseInt(chapterId));
      await request.query(query);
      logger.info(`TranscodingStatus set to 'Completed' for ChapterID: ${chapterId}`);
    });

    logger.info("Adaptive streaming process completed successfully.");
  } catch (error) {
    logger.error("Error during adaptive streaming process:", error);

    // Set TranscodingStatus to 'Failed'
    if (process.env.chapterId) {
      await updateTranscodingError(process.env.chapterId, error.message);
    }

    throw error; // Rethrow to ensure the process fails visibly
  }
};

// Graceful shutdown
const handleShutdown = async () => {
  logger.info("Shutting down gracefully...");
  process.exit(0);
};

process.on("SIGTERM", handleShutdown);
process.on("SIGINT", handleShutdown);

// Start the processing task
processTask()
  .then(() => {
    logger.info("Processing task script reached its end. Exiting gracefully.");
  })
  .catch((error) => {
    logger.error("Unhandled error in processing task:", error);
    process.exit(1);
  });