import ffmpeg from 'fluent-ffmpeg';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import pLimit from 'p-limit';
import winston from 'winston';
import util from 'util';
import stream from 'stream';

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => 
      `${timestamp} [${level}]: ${message}`)
  ),
  transports: [new winston.transports.Console()],
});

const pipeline = util.promisify(stream.pipeline);
const concurrencyLimit = parseInt(process.env.CONCURRENCY_LIMIT) || 2;

function getBandwidth(resolution) {
  const bandwidthMap = {
    "240p": 800000,
    "360p": 1500000,
    "720p": 3000000,
    "1080p": 6000000,
  };
  return bandwidthMap[resolution] || 1000000;
}

export function getVideoDuration(inputPath) {
  return new Promise((resolve, reject) => {
    ffmpeg.ffprobe(inputPath, (err, metadata) => {
      if (err) return reject(err);
      resolve(metadata.format.duration);
    });
  });
}

function getFfmpegCommand(inputPath) {
  return ffmpeg(inputPath)
    .outputOptions([
      '-threads 0',
      '-preset veryfast',
      '-tune fastdecode',
      '-movflags +faststart',
      '-strict experimental',
      '-f hls',
      '-hls_list_size 0',
      '-hls_time 6',
      '-hls_flags independent_segments',
      '-avoid_negative_ts make_zero',
      '-fflags +genpts'
    ]);
}

export async function transcodeVideoForAdaptiveStreaming(inputPath, fileName, chapterId) {
  // Validate input
  try {
    await fs.access(inputPath);
  } catch (error) {
    throw new Error(`Input file not accessible: ${inputPath}`);
  }

  // Create session directory
  const sessionDir = path.join(os.tmpdir(), `transcode_${Date.now()}_${chapterId}`);
  await fs.mkdir(sessionDir, { recursive: true });

  const resolutions = {
    "240p": "426x240",
    "360p": "640x360", 
    "720p": "1280x720",
    "1080p": "1920x1080",
  };
  
  const outputPaths = [];
  const baseName = path.parse(fileName).name.replace(/\s+/g, '_');
  const virtualFolder = `${chapterId}/`;
  const masterPlaylistPath = path.join(sessionDir, `${chapterId}_${baseName}_master.m3u8`);
  const limit = pLimit(concurrencyLimit);

  logger.info(`Starting transcoding session in: ${sessionDir}`, {
    resolutions: Object.keys(resolutions),
    concurrency: concurrencyLimit
  });

  try {
    // Process each resolution
    const transcodePromises = Object.entries(resolutions).map(([resolution, size]) => 
      limit(() => new Promise((resolve, reject) => {
        const outputFileName = `${chapterId}_${baseName}_${resolution}.m3u8`;
        const outputPath = path.join(sessionDir, outputFileName);
        const segmentPrefix = path.join(sessionDir, `${chapterId}_${baseName}_${resolution}_`);
        
        const command = getFfmpegCommand(inputPath)
          .outputOptions([
            `-vf scale=${size}`,
            `-hls_segment_filename ${segmentPrefix}%03d.ts`,
            `-b:v ${getBandwidth(resolution)}`,
            `-maxrate ${getBandwidth(resolution) * 1.2}`,
            `-bufsize ${getBandwidth(resolution) * 2}`
          ])
          .output(outputPath)
          .on("start", (commandLine) => {
            logger.debug(`Starting ${resolution} transcoding`, { command: commandLine });
          })
          .on("progress", (progress) => {
            logger.debug(`${resolution} progress: ${Math.floor(progress.percent)}%`, {
              timemark: progress.timemark,
              fps: progress.currentFps
            });
          })
          .on("end", () => {
            logger.info(`Completed ${resolution} transcoding`);
            outputPaths.push(outputPath);
            resolve();
          })
          .on("error", (err) => {
            logger.error(`${resolution} transcoding failed:`, err);
            reject(new Error(`${resolution} transcode failed: ${err.message}`));
          });

        command.run();
      }))
    );

    await Promise.all(transcodePromises);

    // Generate master playlist
    const masterPlaylistContent = [
      '#EXTM3U',
      '#EXT-X-VERSION:3',
      ...Object.entries(resolutions).map(([res, size]) => 
        `#EXT-X-STREAM-INF:BANDWIDTH=${getBandwidth(res)},RESOLUTION=${size}\n` +
        `${virtualFolder}${chapterId}_${baseName}_${res}.m3u8`
      )
    ].join('\n');

    await fs.writeFile(masterPlaylistPath, masterPlaylistContent);
    outputPaths.push(masterPlaylistPath);

    // Include all segment files
    const segmentFiles = (await fs.readdir(sessionDir))
      .filter(file => file.endsWith('.ts') && file.startsWith(`${chapterId}_${baseName}_`))
      .map(file => path.join(sessionDir, file));
    
    outputPaths.push(...segmentFiles);

    logger.info(`Successfully generated ${outputPaths.length} files`, {
      totalSizeMB: await getTotalSizeMB(outputPaths)
    });
    return outputPaths;
  } catch (error) {
    try {
      await cleanupFiles(outputPaths);
      await fs.rm(sessionDir, { recursive: true, force: true });
    } catch (cleanupError) {
      logger.error("Cleanup error:", cleanupError);
    }
    throw new Error(`Transcoding failed: ${error.message}`);
  }
}

async function getTotalSizeMB(filePaths) {
  try {
    const sizes = await Promise.all(
      filePaths.map(async filePath => {
        try {
          const stat = await fs.stat(filePath);
          return stat.size;
        } catch {
          return 0;
        }
      })
    );
    return sizes.reduce((sum, size) => sum + size, 0) / (1024 * 1024);
  } catch {
    return 0;
  }
}

export async function cleanupFiles(filePaths) {
  await Promise.all(
    filePaths.map(async (filePath) => {
      try {
        await fs.unlink(filePath);
      } catch (error) {
        if (error.code !== 'ENOENT') {
          logger.warn(`Could not delete ${filePath}: ${error.message}`);
        }
      }
    })
  );
}