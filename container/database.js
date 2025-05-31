import pg from 'pg';
const { Pool } = pg;
import winston from 'winston';

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => `${timestamp} [${level}]: ${message}`)
  ),
  transports: [new winston.transports.Console()],
});

const pool = new Pool({
  connectionString: process.env.NEON_POSTGRES_URL,
  min: parseInt(process.env.NEON_POOL_MIN || '1'),
  max: parseInt(process.env.NEON_POOL_MAX || '5'),
  ssl: {
    rejectUnauthorized: false
  },
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000
});

// Connection verification with retry
export async function ensureDatabaseConnection() {
  let attempts = 0;
  const maxAttempts = 3;
  const retryDelay = 1000; // 1 second

  while (attempts < maxAttempts) {
    try {
      const client = await pool.connect();
      await client.query('SELECT 1'); // Simple test query
      client.release();
      logger.info('Database connection verified');
      return true;
    } catch (error) {
      attempts++;
      if (attempts >= maxAttempts) {
        logger.error('Database connection failed after retries:', error);
        throw new Error('Database connection failed');
      }
      logger.warn(`Database connection attempt ${attempts} failed, retrying...`);
      await new Promise(resolve => setTimeout(resolve, retryDelay * attempts));
    }
  }
}

async function ensureChapterFilesTable(client) {
  try {
    // Check if sequence exists, create if not
    await client.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_sequences WHERE sequencename = 'ChapterFiles_FileID_Seq') THEN
          CREATE SEQUENCE "ChapterFiles_FileID_Seq" START 1;
        END IF;
      END $$;
    `);
    
    // Create table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS "ChapterFiles" (
        "FileID" INT DEFAULT nextval('"ChapterFiles_FileID_Seq"') PRIMARY KEY,
        "ChapterID" INT NOT NULL,
        "BlobName" VARCHAR(255) NOT NULL,
        "ContainerName" VARCHAR(255) NOT NULL,
        "CreatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY ("ChapterID") REFERENCES "Courses_Chapters"("ChapterID")
      )
    `);
    
    // Add unique constraint if not exists
    await client.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint 
          WHERE conname = 'unique_chapter_file'
        ) THEN
          ALTER TABLE "ChapterFiles" 
          ADD CONSTRAINT "unique_chapter_file" 
          UNIQUE ("ChapterID", "BlobName");
        END IF;
      END $$;
    `);
    
    logger.info('Verified ChapterFiles table and constraints exist');
  } catch (error) {
    logger.error('Could not ensure ChapterFiles table:', error);
    throw error;
  }
}

export async function withDatabaseConnection(callback) {
  const client = await pool.connect();
  try {
    await ensureChapterFilesTable(client);
    return await callback(client);
  } finally {
    client.release();
  }
}

export async function updateMasterPlaylistInDatabase(chapterId, masterPlaylistUrl) {
  return withDatabaseConnection(async (client) => {
    await client.query(`
      UPDATE "Courses_Chapters"
      SET "Video" = $1,
          "UpdatedAt" = NOW()
      WHERE "ChapterID" = $2
    `, [masterPlaylistUrl, chapterId]);
    logger.info(`Updated master playlist URL for chapter ${chapterId}`);
  });
}

export async function updateTranscodingStatus(chapterId, status, errorMessage = null) {
  return withDatabaseConnection(async (client) => {
    const query = `
      UPDATE "Courses_Chapters"
      SET "TranscodingStatus" = $1,
          "UpdatedAt" = NOW()
          ${errorMessage ? ', "TranscodingError" = $3' : ''}
      WHERE "ChapterID" = $2
    `;
    
    const params = [status, chapterId];
    if (errorMessage) params.push(errorMessage);
    
    await client.query(query, params);
    logger.info(`Updated status to ${status} for chapter ${chapterId}`);
  });
}

export async function insertFileMetadata(chapterId, blobName, containerName) {
  return withDatabaseConnection(async (client) => {
    try {
      await client.query(`
        INSERT INTO "ChapterFiles" ("ChapterID", "BlobName", "ContainerName")
        VALUES ($1, $2, $3)
        ON CONFLICT ON CONSTRAINT "unique_chapter_file" DO NOTHING
      `, [chapterId, blobName, containerName]);
      logger.info(`Recorded metadata for ${blobName} in chapter ${chapterId}`);
    } catch (error) {
      logger.error(`Metadata insertion failed: ${error.message}`);
      throw error;
    }
  });
}

export async function shutdownDatabase() {
  try {
    // Close all idle clients first
    await pool.end();
    logger.info('Database connection pool closed');
  } catch (error) {
    logger.error('Error closing database pool:', error);
    throw error;
  }
}

process.on('SIGTERM', shutdownDatabase);
process.on('SIGINT', shutdownDatabase);