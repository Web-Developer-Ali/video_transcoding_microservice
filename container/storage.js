import { BlobServiceClient } from "@azure/storage-blob";
import { promises as fs, createWriteStream, createReadStream } from "fs";
import { pipeline } from "stream/promises";
import path from "path";
import os from "os";
import mime from "mime-types";
import winston from "winston";

const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      ({ timestamp, level, message }) => `${timestamp} [${level}]: ${message}`
    )
  ),
  transports: [new winston.transports.Console()],
});

const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
if (!connectionString) {
  throw new Error("AZURE_STORAGE_CONNECTION_STRING is required");
}

const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
const MAX_UPLOAD_RETRIES = 3;
const UPLOAD_TIMEOUT_MS = 300000; // 5 minutes
const DOWNLOAD_TIMEOUT_MS = 300000; // 5 minutes

export async function downloadBlob(blobName, containerName) {
  let downloadPath;
  try {
    const containerClient = blobServiceClient.getContainerClient(containerName);
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);
    downloadPath = path.join(os.tmpdir(), blobName);

    // Ensure directory exists
    await fs.mkdir(path.dirname(downloadPath), { recursive: true });

    logger.info(`Downloading blob: ${blobName} from container: ${containerName}`);

    const downloadResponse = await blockBlobClient.download();
    const writeStream = createWriteStream(downloadPath);

    await Promise.race([
      pipeline(downloadResponse.readableStreamBody, writeStream),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Download timeout')), DOWNLOAD_TIMEOUT_MS)
      )
    ]);

    logger.info(`Blob downloaded to: ${downloadPath}`);
    return downloadPath;
  } catch (error) {
    // Clean up partially downloaded file if it exists
    if (downloadPath) {
      await fs.unlink(downloadPath).catch(() => {});
    }
    logger.error(`Error downloading blob ${blobName}:`, error);
    throw new Error(`Failed to download blob: ${blobName} - ${error.message}`);
  }
}

export async function uploadBlobs(filePaths, containerName, chapterId, metadataCallback) {
  try {
    const containerClient = blobServiceClient.getContainerClient(containerName);
    let masterPlaylistUrl = "";
    const virtualFolder = `${chapterId}/`;

    logger.info(`Uploading ${filePaths.length} files to virtual folder: ${virtualFolder}`);

    for (const filePath of filePaths) {
      const absolutePath = path.resolve(filePath);
      const fileName = path.basename(absolutePath);
      const contentType = mime.lookup(fileName) || "application/octet-stream";

      if (!fileName.startsWith(`${chapterId}_`)) {
        logger.warn(`Skipping file not belonging to chapter ${chapterId}: ${fileName}`);
        continue;
      }

      const blobName = `${virtualFolder}${fileName}`;

      try {
        await fs.access(absolutePath);
      } catch (err) {
        logger.error(`File not found: ${absolutePath}`);
        throw new Error(`File not found: ${fileName}`);
      }

      const blockBlobClient = containerClient.getBlockBlobClient(blobName);
      
      let lastError;
      for (let attempt = 1; attempt <= MAX_UPLOAD_RETRIES; attempt++) {
        try {
          const readStream = createReadStream(absolutePath);
          await Promise.race([
            blockBlobClient.uploadStream(readStream, undefined, undefined, {
              blobHTTPHeaders: { blobContentType: contentType }
            }),
            new Promise((_, reject) => 
              setTimeout(() => reject(new Error('Upload timeout')), UPLOAD_TIMEOUT_MS)
            )
          ]);

          if (fileName.endsWith('.m3u8') && typeof metadataCallback === 'function') {
            await metadataCallback(chapterId, blobName, containerName).catch(err => {
              logger.warn(`Non-critical metadata error for ${blobName}: ${err.message}`);
            });
          }

          if (fileName.includes('_master.m3u8')) {
            masterPlaylistUrl = blockBlobClient.url;
            logger.info(`Master playlist URL: ${masterPlaylistUrl}`);
          }
          break;
        } catch (error) {
          lastError = error;
          logger.warn(`Upload attempt ${attempt} failed for ${blobName}`, {
            error: error.message
          });
          if (attempt === MAX_UPLOAD_RETRIES) throw error;
          await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
        }
      }
    }

    if (!masterPlaylistUrl) {
      throw new Error('Master playlist not found in uploaded files');
    }

    return {
      masterPlaylistUrl,
      virtualFolder
    };
  } catch (error) {
    logger.error('Error uploading blobs:', error);
    throw new Error(`Upload failed: ${error.message}`);
  }
}

export async function deleteBlob(blobName, containerName) {
  try {
    const containerClient = blobServiceClient.getContainerClient(containerName);
    const blockBlobClient = containerClient.getBlockBlobClient(blobName);

    logger.info(`Deleting blob: ${blobName} from container: ${containerName}`);
    await blockBlobClient.delete();
    logger.info(`Successfully deleted blob: ${blobName}`);
  } catch (error) {
    logger.error(`Error deleting blob ${blobName}:`, error);
    throw new Error(`Failed to delete blob: ${blobName}`);
  }
}