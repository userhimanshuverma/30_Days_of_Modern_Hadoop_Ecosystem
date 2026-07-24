# Google Cloud Storage (GCS) Hands-On Lab Guide

Step-by-step production lab guide for configuring Google Cloud Storage connector (`gcs-connector`) with Google Service Account keys.

---

## 1. Prerequisites
- GCP Project with Cloud Storage API enabled.
- Google Cloud SDK (`gcloud`) installed and authenticated (`gcloud auth login`).
- Apache Hadoop 3.x cluster with `gcs-connector-hadoop3-*.jar` added to classpath.

---

## 2. Step 1: Create GCS Bucket & Service Account

```bash
# Set Variables
export GCP_PROJECT="my-hadoop-hybrid-project"
export GCS_BUCKET="gs://hadoop-hybrid-lake-$(date +%s)"
export SA_NAME="sa-hadoop-gcs"

# Set Active Project
gcloud config set project ${GCP_PROJECT}

# Create Storage Bucket
gcloud storage buckets create ${GCS_BUCKET} \
    --location=us-central1 \
    --uniform-bucket-level-access

# Create Service Account
gcloud iam service-accounts create ${SA_NAME} \
    --display-name="Hadoop GCS Service Account"

# Grant Storage Admin Role
gcloud projects add-iam-policy-binding ${GCP_PROJECT} \
    --member="serviceAccount:${SA_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

# Download JSON Private Key
gcloud iam service-accounts keys create /etc/gcp/gcp-sa-key.json \
    --iam-account="${SA_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com"
```

---

## 3. Step 2: Configure `core-site.xml` for GCS

```xml
<property>
    <name>fs.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem</value>
</property>
<property>
    <name>fs.AbstractFileSystem.gs.impl</name>
    <value>com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS</value>
</property>
<property>
    <name>fs.gs.project.id</name>
    <value>my-hadoop-hybrid-project</value>
</property>
<property>
    <name>fs.gs.auth.service.account.enable</name>
    <value>true</value>
</property>
<property>
    <name>fs.gs.auth.service.account.json.keyfile</name>
    <value>/etc/gcp/gcp-sa-key.json</value>
</property>
```

---

## 4. Step 3: Verification & Execution

```bash
# List Bucket
hdfs dfs -ls ${GCS_BUCKET}/

# Put Payload
echo "Google Cloud Storage Verification" > gcs.txt
hdfs dfs -put gcs.txt ${GCS_BUCKET}/gcs.txt

# Read back
hdfs dfs -cat ${GCS_BUCKET}/gcs.txt
```

---

## 5. Cleanup

```bash
gcloud storage rm --recursive ${GCS_BUCKET}
rm -f /etc/gcp/gcp-sa-key.json
```
