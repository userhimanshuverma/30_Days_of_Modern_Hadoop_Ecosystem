# AWS S3 Hands-On Lab Guide

Step-by-step production lab guide for configuring and verifying Hadoop S3A integration with Amazon S3.

---

## 1. Prerequisites
- AWS Account with permissions to manage IAM and S3.
- AWS CLI v2 installed and configured (`aws configure`).
- Apache Hadoop 3.x cluster or Docker environment with `hadoop-aws` JAR.

---

## 2. Step 1: Create AWS S3 Bucket & IAM Policy

```bash
# Set bucket name variable
export S3_BUCKET="hadoop-hybrid-lake-$(date +%s)"
export AWS_REGION="us-east-1"

# Create S3 Bucket
aws s3api create-bucket \
    --bucket ${S3_BUCKET} \
    --region ${AWS_REGION}

# Enable Default Encryption (SSE-S3)
aws s3api put-bucket-encryption \
    --bucket ${S3_BUCKET} \
    --server-side-encryption-configuration '{
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
    }'

# Block Public Access
aws s3api put-public-access-block \
    --bucket ${S3_BUCKET} \
    --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

---

## 3. Step 2: Create IAM User & Policy for S3A

```bash
# Create IAM Policy JSON
cat << 'EOF' > s3a-policy.json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:ListBucketMultipartUploads"
            ],
            "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
            ],
            "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME/*"
        }
    ]
}
EOF

sed -i "s/YOUR_BUCKET_NAME/${S3_BUCKET}/g" s3a-policy.json

# Create IAM Policy
aws iam create-policy \
    --policy-name HadoopS3AccessPolicy \
    --policy-document file://s3a-policy.json
```

---

## 4. Step 3: Configure `core-site.xml`

Add the following properties to `$HADOOP_CONF_DIR/core-site.xml`:

```xml
<property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
</property>
<property>
    <name>fs.s3a.endpoint</name>
    <value>s3.us-east-1.amazonaws.com</value>
</property>
<property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>com.amazonaws.auth.EnvironmentVariableCredentialsProvider</value>
</property>
<property>
    <name>fs.s3a.fast.upload</name>
    <value>true</value>
</property>
<property>
    <name>fs.s3a.committer.name</name>
    <value>magic</value>
</property>
```

---

## 5. Step 4: Verification Commands

```bash
# Set credentials in session
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="secret..."

# List S3 Bucket via Hadoop CLI
hdfs dfs -ls s3a://${S3_BUCKET}/

# Write test file
echo "AWS S3A Verification Data" > test.txt
hdfs dfs -put test.txt s3a://${S3_BUCKET}/test.txt

# Read back
hdfs dfs -cat s3a://${S3_BUCKET}/test.txt
```

---

## 6. Cleanup

```bash
hdfs dfs -rm -r s3a://${S3_BUCKET}/test.txt
aws s3 rb s3://${S3_BUCKET} --force
```
