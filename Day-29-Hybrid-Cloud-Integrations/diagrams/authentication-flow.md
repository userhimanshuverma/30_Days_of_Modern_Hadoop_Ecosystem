# Hybrid Cloud Authentication Flow Diagrams

Exhaustive authentication diagrams for enterprise hybrid cloud setups, detailing IRSA (AWS IAM Roles for Service Accounts), Azure Entra ID Service Principals, and GCP Workload Identity.

---

## 1. AWS IRSA (IAM Roles for Service Accounts) Authentication Flow

```mermaid
sequenceDiagram
    autonumber
    participant Pod as Spark Executor Pod (Kubernetes / YARN)
    participant OIDC as AWS IAM OIDC Identity Provider
    participant STS as AWS Security Token Service (STS)
    participant S3 as AWS S3 Storage Service

    Pod->>Pod: Read ServiceAccount Token from /var/run/secrets/tokens/jwt
    Pod->>STS: AssumeRoleWithWebIdentity(RoleArn, WebIdentityToken)
    STS->>OIDC: Validate K8s JWT Signature & Issuer
    OIDC-->>STS: Token Validated
    STS-->>Pod: Return Temporary AWS Credentials (AccessKey, SecretKey, SessionToken - 1 hour expiry)
    Pod->>S3: GET /bucket/data.parquet (Authenticated with Temporary Credentials)
    S3-->>Pod: 200 OK
```

---

## 2. Azure Service Principal & Managed Identity Authentication Flow

```mermaid
sequenceDiagram
    autonumber
    participant App as Hadoop / Spark Node
    participant Entra as Microsoft Entra ID (Azure AD)
    participant ADLS as ADLS Gen2 Storage Account

    App->>Entra: POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
    Note over App,Entra: Grant Type: client_credentials (client_id + client_secret / certificate)
    Entra-->>App: Return OAuth2 Access Token (access_token: eyJhbGci...)
    App->>ADLS: GET https://{account}.dfs.core.windows.net/{container}/path
    Note over App,ADLS: Header: Authorization: Bearer eyJhbGci...
    ADLS-->>App: 200 OK + Data Payload
```

---

## 3. GCP Workload Identity Federation Flow

```mermaid
sequenceDiagram
    autonumber
    participant Spark as Spark Container (On-Prem / Kubernetes)
    participant GCP_STS as GCP Security Token Service
    participant GCP_IAM as GCP IAM Credentials API
    participant GCS as Google Cloud Storage

    Spark->>GCP_STS: Exchange Local ID Token for GCP Federated Token
    GCP_STS-->>Spark: Return Federated Access Token
    Spark->>GCP_IAM: Generate ServiceAccount AccessToken (impersonation)
    GCP_IAM-->>Spark: Return Short-lived GCP OAuth Access Token
    Spark->>GCS: Read Object gs://bucket/file.parquet (Authorization: Bearer Token)
    GCS-->>Spark: 200 OK
```
