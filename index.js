const { QueueServiceClient } = require("@azure/storage-queue");
const { BlobServiceClient } = require("@azure/storage-blob");
const { ContainerInstanceManagementClient } = require("@azure/arm-containerinstance");
const { DefaultAzureCredential } = require("@azure/identity");
require("dotenv").config();

// Validate required environment variables
const requiredEnvVars = [
  "AZURE_STORAGE_CONNECTION_STRING",
  "AZURE_QUEUE_NAME",
  "INPUT_CONTAINER_NAME",
  "AZURE_SUBSCRIPTION_ID",
  "AZURE_RESOURCE_GROUP",
  "AZURE_SQL_USER",
  "AZURE_SQL_PASSWORD",
  "AZURE_SQL_SERVER",
  "AZURE_SQL_DATABASE",
  "AZURE_STORAGE_ACCOUNT_NAME",
  "AZURE_STORAGE_ACCOUNT_KEY",
  "AZURE_REGISTRY_PASSWORD",
  "OUTPUT_CONTAINER_NAME",
  "CONCURRENCY_LIMIT"
];

for (const envVar of requiredEnvVars) {
  if (!process.env[envVar]) {
    console.error(`Missing required environment variable: ${envVar}`);
    process.exit(1);
  }
}

// Initialize Azure Clients
const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
const queueName = process.env.AZURE_QUEUE_NAME;
const containerName = process.env.INPUT_CONTAINER_NAME;
const queueServiceClient = QueueServiceClient.fromConnectionString(connectionString);
const queueClient = queueServiceClient.getQueueClient(queueName);
const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
const blobContainerClient = blobServiceClient.getContainerClient(containerName);

// Initialize Azure Container Management Client
const subscriptionId = process.env.AZURE_SUBSCRIPTION_ID;
const resourceGroupName = process.env.AZURE_RESOURCE_GROUP;
const containerGroupName = "transcodingcontainer";
const credentials = new DefaultAzureCredential();
const client = new ContainerInstanceManagementClient(credentials, subscriptionId);

// Check if container group exists
async function checkContainerGroupExists() {
  try {
    await client.containerGroups.get(resourceGroupName, containerGroupName);
    return true;
  } catch (error) {
    return error.statusCode === 404 ? false : (() => { throw error; })();
  }
}

// Fetch Chapter ID from Blob Metadata
async function getChapterIdFromBlob(blobName) {
  try {
    const blobClient = blobContainerClient.getBlobClient(blobName);
    const properties = await blobClient.getProperties();
    return properties.metadata?.chapterid || null;
  } catch (error) {
    console.error(`Error fetching metadata for blob ${blobName}:`, error.message);
    return null;
  }
}

// Delete Container Group if Exists
async function deleteContainerGroupIfExists() {
  if (await checkContainerGroupExists()) {
    try {
      const deleteResult = await client.containerGroups.beginDelete(resourceGroupName, containerGroupName);
      await deleteResult.pollUntilDone();
      console.log("Deleted existing container group:", containerGroupName);
    } catch (error) {
      console.error("Error deleting container group:", error.message);
    }
  }
}

// Create or Update Container Group
async function createOrUpdateContainerGroup(messageContent, chapterId) {
  const envVars = [
    { name: "AZURE_STORAGE_CONNECTION_STRING", value: connectionString },
    { name: "MESSAGE_CONTENT", value: Buffer.from(messageContent).toString("base64") },
    { name: "AZURE_SQL_USER", value: process.env.AZURE_SQL_USER },
    { name: "AZURE_SQL_PASSWORD", value: process.env.AZURE_SQL_PASSWORD },
    { name: "AZURE_SQL_SERVER", value: process.env.AZURE_SQL_SERVER },
    { name: "AZURE_SQL_DATABASE", value: process.env.AZURE_SQL_DATABASE },
    { name: "INPUT_CONTAINER_NAME", value: containerName },
    { name: "OUTPUT_CONTAINER_NAME", value: process.env.OUTPUT_CONTAINER_NAME },
    { name: "CONCURRENCY_LIMIT", value: process.env.CONCURRENCY_LIMIT },
    { name: "chapterId", value: chapterId },
  ];

  const containerGroup = {
    location: "centralindia",
    osType: "Linux",
    restartPolicy: "Never",
    containers: [
      {
        name: containerGroupName,
        image: "skillspheremicroservice.azurecr.io/skillsphere-image:latest",
        resources: { requests: { cpu: 2, memoryInGB: 4 } },
        environmentVariables: envVars,
        volumeMounts: [{ name: "transcoding-volume", mountPath: "/mnt/storage" }],
      },
    ],
    volumes: [
      {
        name: "transcoding-volume",
        azureFile: {
          shareName: "skillsphere-video-share",
          storageAccountName: process.env.AZURE_STORAGE_ACCOUNT_NAME,
          storageAccountKey: process.env.AZURE_STORAGE_ACCOUNT_KEY,
        },
      },
    ],
    imageRegistryCredentials: [
      {
        server: "skillspheremicroservice.azurecr.io",
        username: "skillspheremicroservice",
        password: process.env.AZURE_REGISTRY_PASSWORD,
      },
    ],
  };

  await deleteContainerGroupIfExists();

  try {
    const result = await client.containerGroups.beginCreateOrUpdate(resourceGroupName, containerGroupName, containerGroup);
    await result.pollUntilDone();
    console.log(`Container group ${containerGroupName} created successfully.`);
  } catch (error) {
    console.error(`Error creating/updating container group:`, error.message);
  }
}

// Start Container with Retry Logic
async function startContainerWithRetry(messageContent, chapterId, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await createOrUpdateContainerGroup(messageContent, chapterId);
      console.log(`Container group ${containerGroupName} started.`);
      return;
    } catch (error) {
      if (attempt === retries) {
        console.error("Failed after multiple attempts:", error.message);
      } else {
        console.log(`Retrying start attempt ${attempt}...`);
      }
    }
  }
}

// Stop Container Group
async function stopContainer() {
  if (await checkContainerGroupExists()) {
    try {
      await client.containerGroups.stop(resourceGroupName, containerGroupName);
      console.log(`Container group ${containerGroupName} stopped.`);
    } catch (error) {
      console.error("Error stopping container group:", error.message);
    }
  }
}

// Process Messages in Queue
async function processMessages() {
  while (true) {
    try {
      await queueClient.createIfNotExists();
      console.log(`Connected to queue: ${queueName}`);

      const response = await queueClient.receiveMessages({ numberOfMessages: 3, visibilityTimeout: 30 });

      if (!response.receivedMessageItems.length) {
        console.log("No messages available.");
        await new Promise((resolve) => setTimeout(resolve, 5000)); // Wait before polling again
        continue;
      }

      await Promise.all(
        response.receivedMessageItems.map(async (message) => {
          try {
            const decodedMessage = Buffer.from(message.messageText, "base64").toString("utf8");
            console.log(`Received message: ${decodedMessage}`);

            const { data: { url } } = JSON.parse(decodedMessage);
            const blobName = url.split("/").pop();
            const chapterId = await getChapterIdFromBlob(blobName);

            if (chapterId) {
              await startContainerWithRetry(decodedMessage, chapterId);
            } else {
              console.warn(`Skipping message due to missing chapterId.`);
            }

            await queueClient.deleteMessage(message.messageId, message.popReceipt);
            console.log(`Message processed and deleted.`);
          } catch (error) {
            console.error("Error processing message:", error.message);
          }
        })
      );
    } catch (error) {
      console.error("Error receiving messages:", error.message);
      await new Promise((resolve) => setTimeout(resolve, 5000)); // Wait before retrying
    }
  }
}

// Start processing
processMessages().catch(console.error);
