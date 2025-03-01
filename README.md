# **Video Transcoding Microservice**

This repository contains a **microservice** designed to handle video transcoding tasks for your product. It leverages Azure services to process video files, transcode them into adaptive streaming formats, and manage the lifecycle of containerized transcoding tasks.

---

## **Features**
- **Video Transcoding**: Transcodes video files into multiple resolutions (240p, 360p, 720p, 1080p) for adaptive streaming.
- **Containerized Processing**: Uses Azure Container Instances to run transcoding tasks in isolated environments.
- **Queue-Based Workflow**: Processes messages from an Azure Storage Queue to trigger transcoding tasks.
- **Metadata Management**: Tracks video metadata (e.g., chapter ID) in Azure SQL Database.
- **Automatic Cleanup**: Deletes temporary files and container groups after processing.

---

## **Prerequisites**
Before running the project, ensure you have the following:

1. **Azure Account**: An active Azure subscription.
2. **Azure Resources**:
   - Azure Storage Account (for Blob Storage and Queues).
   - Azure Container Registry (for storing the transcoding container image).
   - Azure SQL Database (for storing metadata).
3. **Environment Variables**:
   - Set up the required environment variables (see [Configuration](#configuration)).

---

## **Configuration**

### **Environment Variables**
Create a `.env` file in the root directory with the following variables:

```plaintext
# Azure Storage
AZURE_STORAGE_CONNECTION_STRING=<Your Azure Storage Connection String>
AZURE_STORAGE_ACCOUNT_NAME=<Your Storage Account Name>
AZURE_STORAGE_ACCOUNT_KEY=<Your Storage Account Key>

# Azure Queue
AZURE_QUEUE_NAME=<Your Queue Name>

# Azure Container Instances
AZURE_SUBSCRIPTION_ID=<Your Azure Subscription ID>
AZURE_RESOURCE_GROUP=<Your Resource Group Name>
AZURE_REGISTRY_PASSWORD=<Your Azure Container Registry Password>

# Azure SQL Database
AZURE_SQL_USER=<Your SQL Database Username>
AZURE_SQL_PASSWORD=<Your SQL Database Password>
AZURE_SQL_SERVER=<Your SQL Server Name>
AZURE_SQL_DATABASE=<Your SQL Database Name>

# Transcoding Configuration
INPUT_CONTAINER_NAME=<Input Blob Container Name>
OUTPUT_CONTAINER_NAME=<Output Blob Container Name>
CONCURRENCY_LIMIT=<Number of Concurrent Transcoding Tasks>
```

---

## **Setup**

### **1. Build and Push the Transcoding Container**
1. Build the Docker image for the transcoding service:
   ```bash
   docker build -t skillspheremicroservice.azurecr.io/skillsphere-image:latest .
   ```
2. Push the image to your Azure Container Registry:
   ```bash
   docker push skillspheremicroservice.azurecr.io/skillsphere-image:latest
   ```

### **2. Deploy the Microservice**
1. Install dependencies:
   ```bash
   npm install
   ```
2. Start the microservice:
   ```bash
   node index.js
   ```

---

## **Workflow**

1. **Queue Trigger**:
   - Messages are added to the Azure Storage Queue with details about the video file to transcode.
   - Each message contains a URL to the video file in Azure Blob Storage.

2. **Blob Metadata**:
   - The system retrieves the `chapterId` from the video file's metadata in Azure Blob Storage.

3. **Containerized Transcoding**:
   - A new Azure Container Instance is created to handle the transcoding task.
   - The container processes the video file and generates multiple resolutions for adaptive streaming.

4. **Output Storage**:
   - The transcoded files are uploaded to the output container in Azure Blob Storage.
   - The master playlist URL is updated in the Azure SQL Database.

5. **Cleanup**:
   - Temporary files and container groups are deleted after processing.

---

## **Usage**

### **Adding a Video to the Queue**
To trigger a transcoding task, add a message to the Azure Storage Queue with the following format:

```json
{
  "data": {
    "url": "https://<storage-account>.blob.core.windows.net/<container>/<video-file>"
  }
}
```

### **Monitoring**
- Use Azure Monitor to track the status of container instances and transcoding tasks.
- Check the logs in the output container for transcoded files.

---

## **Troubleshooting**

### **Common Issues**
1. **Missing Environment Variables**:
   - Ensure all required environment variables are set in the `.env` file.

2. **Container Group Creation Fails**:
   - Verify that the Azure Container Registry credentials are correct.
   - Check the resource limits (CPU, memory) for the container group.

3. **Transcoding Errors**:
   - Ensure the input video file is in a supported format.
   - Check the logs of the container instance for detailed error messages.

---

## **Contributing**
Contributions are welcome! Please follow these steps:
1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a detailed description of your changes.

---

## **License**
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

## **About This Microservice**
This microservice is part of our product's backend infrastructure, designed to handle video transcoding tasks efficiently. It is built to be scalable, reliable, and easy to integrate with other components of the system.

---

This updated README emphasizes that the repository is a **microservice** for handling video transcoding in your product. Let me know if you need further adjustments!
