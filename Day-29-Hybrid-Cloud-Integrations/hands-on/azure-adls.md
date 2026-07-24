# Azure ADLS Gen2 Hands-On Lab Guide

Step-by-step production lab guide for configuring Azure Data Lake Storage Gen2 (ABFS driver) with Service Principal authentication.

---

## 1. Prerequisites
- Azure Subscription with Owner or User Access Administrator role.
- Azure CLI (`az`) installed and authenticated (`az login`).
- Apache Hadoop 3.x cluster with `hadoop-azure` and `azure-data-lake-store-sdk` JARs.

---

## 2. Step 1: Create Storage Account with Hierarchical Namespace (HNS)

```bash
# Variables
export RESOURCE_GROUP="rg-hadoop-hybrid"
export LOCATION="eastus"
export STORAGE_ACCOUNT="stgdatalake$(date +%s | cut -c 6-10)"
export CONTAINER_NAME="analytics"

# Create Resource Group
az group create --name ${RESOURCE_GROUP} --location ${LOCATION}

# Create ADLS Gen2 Storage Account (enable-hierarchical-namespace is REQUIRED)
az storage account create \
    --name ${STORAGE_ACCOUNT} \
    --resource-group ${RESOURCE_GROUP} \
    --location ${LOCATION} \
    --sku Standard_LRS \
    --kind StorageV2 \
    --enable-hierarchical-namespace true

# Create Container
az storage container create \
    --name ${CONTAINER_NAME} \
    --account-name ${STORAGE_ACCOUNT} \
    --auth-mode login
```

---

## 3. Step 2: Create Microsoft Entra Service Principal & Grant Role

```bash
# Create Service Principal
SP_JSON=$(az ad sp create-for-rbac --name "sp-hadoop-abfs" --skip-assignment --output json)

export CLIENT_ID=$(echo $SP_JSON | jq -r '.appId')
export CLIENT_SECRET=$(echo $SP_JSON | jq -r '.password')
export TENANT_ID=$(echo $SP_JSON | jq -r '.tenant')

# Get Storage Account Resource ID
STORAGE_ID=$(az storage account show --name ${STORAGE_ACCOUNT} --query id -o tsv)

# Assign Storage Blob Data Contributor Role to Service Principal
az role assignment create \
    --assignee ${CLIENT_ID} \
    --role "Storage Blob Data Contributor" \
    --scope ${STORAGE_ID}
```

---

## 4. Step 3: Configure `core-site.xml` for ABFS

```xml
<property>
    <name>fs.abfs.impl</name>
    <value>org.apache.hadoop.fs.azureblob.AzureBlobFileSystem</value>
</property>
<property>
    <name>fs.abfss.impl</name>
    <value>org.apache.hadoop.fs.azureblob.SecureAzureBlobFileSystem</value>
</property>
<property>
    <name>fs.azure.account.auth.type</name>
    <value>OAuth</value>
</property>
<property>
    <name>fs.azure.account.oauth.provider.type</name>
    <value>org.apache.hadoop.fs.azureblob.oauth2.ClientCredsTokenProvider</value>
</property>
<property>
    <name>fs.azure.account.oauth2.client.id</name>
    <value>YOUR_CLIENT_ID</value>
</property>
<property>
    <name>fs.azure.account.oauth2.client.secret</name>
    <value>YOUR_CLIENT_SECRET</value>
</property>
<property>
    <name>fs.azure.account.oauth2.client.endpoint</name>
    <value>https://login.microsoftonline.com/YOUR_TENANT_ID/oauth2/token</value>
</property>
```

---

## 5. Step 4: Verification & Execution

```bash
# Set ABFS Path URL format: abfs://<container>@<account_name>.dfs.core.windows.net/
export ABFS_URI="abfs://${CONTAINER_NAME}@${STORAGE_ACCOUNT}.dfs.core.windows.net"

# List directory
hdfs dfs -ls ${ABFS_URI}/

# Write file
echo "ADLS Gen2 ABFS Test" > adls.txt
hdfs dfs -put adls.txt ${ABFS_URI}/adls.txt

# Read back
hdfs dfs -cat ${ABFS_URI}/adls.txt
```

---

## 6. Cleanup

```bash
az group delete --name ${RESOURCE_GROUP} --yes --no-wait
```
