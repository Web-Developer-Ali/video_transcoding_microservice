import { QueueServiceClient } from "@azure/storage-queue"
import { BlobServiceClient } from "@azure/storage-blob";
import { ContainerInstanceManagementClient } from "@azure/arm-containerinstance";
import { DefaultAzureCredential } from  "@azure/identity";
import dotenv  from "dotenv";
import winston  from "winston";
import { Pool } from "pg";

dotenv.config();

// Logger Setup
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  transports: [new winston.transports.Console()]
});

// Environment Validation
const requiredEnvVars = [
  "AZURE_STORAGE_CONNECTION_STRING",
  "AZURE_QUEUE_NAME",
  "INPUT_CONTAINER_NAME",
  "AZURE_SUBSCRIPTION_ID",
  "AZURE_RESOURCE_GROUP",
  "NEON_POSTGRES_URL",
  "AZURE_STORAGE_ACCOUNT_NAME",
  "OUTPUT_CONTAINER_NAME",
  "CONCURRENCY_LIMIT",
  "AZURE_REGISTRY_SERVER",
  "AZURE_REGISTRY_USERNAME",
  "AZURE_REGISTRY_PASSWORD"
];

const missingVars = requiredEnvVars.filter(env => !process.env[env]);
if (missingVars.length > 0) {
  logger.error(`Missing required environment variables: ${missingVars.join(", ")}`);
  process.exit(1);
}

// Database Connection
const pool = new Pool({
  connectionString: process.env.NEON_POSTGRES_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

// Azure Clients Setup
const credential = new DefaultAzureCredential();
const queueClient = QueueServiceClient
  .fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING)
  .getQueueClient(process.env.AZURE_QUEUE_NAME);

const blobServiceClient = BlobServiceClient
  .fromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING);

const aciClient = new ContainerInstanceManagementClient(
  credential,
  process.env.AZURE_SUBSCRIPTION_ID
);

// Constants
const CONTAINER_GROUP_PREFIX = "transcoding-group-";
const MAX_RETRIES = 3;
const MAX_CONCURRENT_JOBS = 2;
const QUEUE_VISIBILITY_TIMEOUT = 300;
const HEALTH_CHECK_PORT = 8080;
const AZURE_LOCATION = process.env.AZURE_LOCATION || "centralindia";
const JOB_CHECK_INTERVAL = 30000;

// Track active jobs
const activeJobs = new Map();

async function getChapterIdFromBlob(blobName) {
  const blobClient = blobServiceClient
    .getContainerClient(process.env.INPUT_CONTAINER_NAME)
    .getBlobClient(blobName);

  try {
    const properties = await blobClient.getProperties();
    return properties.metadata?.chapterid || null;
  } catch (error) {
    logger.error('Error fetching blob metadata', { blobName, error: error.message });
    return null;
  }
}

async function cleanupContainerGroup(containerGroupName) {
  try {
    const poller = await aciClient.containerGroups.beginDelete(
      process.env.AZURE_RESOURCE_GROUP,
      containerGroupName
    );
    await poller.pollUntilDone();
    logger.info('Container group deleted successfully', { containerGroupName });
  } catch (error) {
    if (error.statusCode !== 404) {
      logger.error('Failed to delete container group', { containerGroupName, error: error.message });
    }
  }
}

async function verifyChapterExists(chapterId) {
  try {
    const client = await pool.connect();
    const result = await client.query(
      'SELECT 1 FROM "Courses_Chapters" WHERE "ChapterID" = $1',
      [chapterId]
    );
    client.release();
    return result.rows.length > 0;
  } catch (error) {
    logger.error('Error verifying chapter', { chapterId, error });
    return false;
  }
}

async function updateChapterStatus(chapterId, status) {
  try {
    const client = await pool.connect();
    await client.query(
      'UPDATE "Courses_Chapters" SET "TranscodingStatus" = $1 WHERE "ChapterID" = $2',
      [status, chapterId]
    );
    client.release();
  } catch (error) {
    logger.error('Error updating chapter status', { chapterId, error });
  }
}

// Container Configuration
function getContainerConfig(messageContent, chapterId, containerGroupName) {
  return {
    name: containerGroupName,
    location: AZURE_LOCATION,
    osType: "Linux",
    restartPolicy: "Never",
    containers: [{
      name: `transcoding-container-${chapterId}`,
      image: `${process.env.AZURE_REGISTRY_SERVER}/skillsphere-image:latest`,
      resources: {
        requests: { cpu: 2, memoryInGB: 4 },
      },
      environmentVariables: [
        { name: "AZURE_STORAGE_CONNECTION_STRING", secureValue: process.env.AZURE_STORAGE_CONNECTION_STRING },
        { name: "AZURE_STORAGE_ACCOUNT_NAME", value: process.env.AZURE_STORAGE_ACCOUNT_NAME },
        { name: "MESSAGE_CONTENT", secureValue: Buffer.from(messageContent).toString("base64") },
        { name: "NEON_POSTGRES_URL", secureValue: process.env.NEON_POSTGRES_URL },
        { name: "INPUT_CONTAINER_NAME", value: process.env.INPUT_CONTAINER_NAME },
        { name: "OUTPUT_CONTAINER_NAME", value: process.env.OUTPUT_CONTAINER_NAME },
        { name: "CONCURRENCY_LIMIT", value: process.env.CONCURRENCY_LIMIT || "2" },
        { name: "chapterId", value: chapterId },
        { name: "LOG_LEVEL", value: process.env.LOG_LEVEL || "info" }
      ],
      ports: [{ port: HEALTH_CHECK_PORT }],
      livenessProbe: {
        httpGet: { path: "/health", port: HEALTH_CHECK_PORT, scheme: "HTTP" },
        initialDelaySeconds: 600,
        periodSeconds: 60,
        failureThreshold: 5,
        timeoutSeconds: 10
      }
    }],
    imageRegistryCredentials: [{
      server: process.env.AZURE_REGISTRY_SERVER,
      username: process.env.AZURE_REGISTRY_USERNAME,
      password: process.env.AZURE_REGISTRY_PASSWORD
    }],
    tags: {
      "Application": "VideoTranscoder",
      "ChapterID": chapterId,
      "Environment": process.env.ENVIRONMENT || "development"
    }
  };
}

// Job Monitoring
async function monitorContainerCompletion(containerGroupName, chapterId) {
  try {
    const checkStatus = async () => {
      try {
        const currentState = await aciClient.containerGroups.get(
          process.env.AZURE_RESOURCE_GROUP,
          containerGroupName
        );
        
        if (currentState.containers.every(c => c.instanceView?.currentState?.state === 'Terminated')) {
          const job = activeJobs.get(containerGroupName);
          if (job && job.timeout) clearTimeout(job.timeout);
          
          activeJobs.delete(containerGroupName);
          await cleanupContainerGroup(containerGroupName);
          
          const success = currentState.containers.every(c => 
            c.instanceView?.currentState?.exitCode === 0
          );
          
          if (success) {
            await updateChapterStatus(chapterId, "Completed");
            logger.info('Transcoding completed successfully', { containerGroupName, chapterId });
          } else {
            await updateChapterStatus(chapterId, "Failed");
            logger.error('Transcoding failed', { containerGroupName, chapterId });
          }
        } else {
          const timeout = setTimeout(checkStatus, JOB_CHECK_INTERVAL);
          activeJobs.set(containerGroupName, { chapterId, timeout });
        }
      } catch (error) {
        logger.error('Error monitoring container', { containerGroupName, chapterId, error: error.message });
        activeJobs.delete(containerGroupName);
      }
    };

    await checkStatus();
  } catch (error) {
    logger.error('Error starting container monitoring', { containerGroupName, chapterId, error: error.message });
    activeJobs.delete(containerGroupName);
  }
}

// Message Processing
async function processMessageWithRetry(message, retries = MAX_RETRIES) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      if (activeJobs.size >= MAX_CONCURRENT_JOBS) {
        throw new Error(`Maximum concurrent jobs (${MAX_CONCURRENT_JOBS}) reached`);
      }

      const decodedMessage = Buffer.from(message.messageText, 'base64').toString('utf-8');
      const messageData = JSON.parse(decodedMessage);
      
      if (!messageData?.data?.url) throw new Error("Invalid message format - missing URL");

      const blobName = messageData.data.url.split("/").pop();
      if (!blobName) throw new Error("Could not extract blob name from URL");

      const chapterId = await getChapterIdFromBlob(blobName);
      if (!chapterId) {
        logger.warn("Skipping message - no chapterId in blob metadata", { blobName });
        await queueClient.deleteMessage(message.messageId, message.popReceipt);
        return;
      }

      if (!await verifyChapterExists(chapterId)) {
        logger.error(`Chapter ${chapterId} does not exist in database`);
        await queueClient.deleteMessage(message.messageId, message.popReceipt);
        return;
      }

      await updateChapterStatus(chapterId, "Processing");
      
      const containerGroupName = await deployContainer(decodedMessage, chapterId);
      await queueClient.deleteMessage(message.messageId, message.popReceipt);
      
      logger.info(`Processed message successfully`, { chapterId, containerGroupName });
      return;
    } catch (error) {
      if (attempt === retries) {
        logger.error('Max retries reached for message', { error: error.message });
        throw error;
      }
      
      const delay = 5000 * attempt;
      logger.warn(`Retrying message (attempt ${attempt}/${retries})`, { error: error.message, delay });
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

async function deployContainer(messageContent, chapterId) {
  const containerGroupName = `${CONTAINER_GROUP_PREFIX}${chapterId}-${Date.now()}`;
  
  try {
    const containerGroup = getContainerConfig(messageContent, chapterId, containerGroupName);
    logger.info('Deploying new container group', { chapterId, containerGroupName });
    
    activeJobs.set(containerGroupName, { chapterId, timeout: setTimeout(() => {}, 0) });
    
    const poller = await aciClient.containerGroups.beginCreateOrUpdate(
      process.env.AZURE_RESOURCE_GROUP,
      containerGroupName,
      containerGroup
    );

    await poller.pollUntilDone();
    logger.info('Container group deployed successfully', { chapterId, containerGroupName });
    
    monitorContainerCompletion(containerGroupName, chapterId);
    
    return containerGroupName;
  } catch (error) {
    activeJobs.delete(containerGroupName);
    await updateChapterStatus(chapterId, "Failed");
    logger.error('Container deployment failed', { chapterId, containerGroupName, error: error.message });
    throw error;
  }
}

// Queue Processing
async function processMessages() {
  await queueClient.createIfNotExists();
  logger.info('Queue listener started');

  while (true) {
    try {
      if (activeJobs.size >= MAX_CONCURRENT_JOBS) {
        await new Promise(resolve => setTimeout(resolve, JOB_CHECK_INTERVAL));
        continue;
      }

      const response = await queueClient.receiveMessages({
        numberOfMessages: 5,
        visibilityTimeout: QUEUE_VISIBILITY_TIMEOUT
      });

      if (!response.receivedMessageItems.length) {
        await new Promise(resolve => setTimeout(resolve, 5000));
        continue;
      }

      // Add 3-second delay after finding messages
      logger.info('Messages found in queue, waiting 3 seconds before processing...');
      await new Promise(resolve => setTimeout(resolve, 3000));

      await Promise.allSettled(
        response.receivedMessageItems.map(msg => 
          processMessageWithRetry(msg).catch(error => 
            logger.error('Message processing failed', { error: error.message })
          )
        )
      );
    } catch (error) {
      logger.error('Queue processing error', { error: error.message });
      await new Promise(resolve => setTimeout(resolve, 10000));
    }
  }
}

// Graceful Shutdown
async function handleShutdown(signal) {
  logger.info(`Shutdown signal received`, { signal });
  
  try {
    const cleanupPromises = Array.from(activeJobs.keys()).map(containerGroupName => 
      cleanupContainerGroup(containerGroupName)
    );
    
    await Promise.all(cleanupPromises);
    await pool.end();
    
    logger.info('Service shutdown completed');
    process.exit(0);
  } catch (error) {
    logger.error('Shutdown failed', { error: error.message });
    process.exit(1);
  }
}

process.on("SIGTERM", () => handleShutdown('SIGTERM'));
process.on("SIGINT", () => handleShutdown('SIGINT'));
process.on("unhandledRejection", (reason, promise) => {
  logger.error("Unhandled Rejection at:", { promise, reason });
});
process.on("uncaughtException", (error) => {
  logger.error("Uncaught Exception:", { error });
  handleShutdown('UNCAUGHT_EXCEPTION');
});

// Startup
processMessages().catch((error) => {
  logger.error("Fatal error in message processing", { error: error.message });
  process.exit(1);
});