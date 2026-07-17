# Hands-On Lab: Deploying & Securing Hadoop with Apache Knox

This hands-on lab guides you through the process of setting up Apache Knox Security Gateway, configuring LDAP-based authentication, and exposing WebHDFS and HiveServer2 services.

---

## 🔬 Lab Architecture Overview

We will spin up a local development environment using Docker Compose:

```
                  +----------------------------------------------+
                  |               Docker Network                 |
                  |                                              |
                  |     +--------------+   +-----------------+   |
                  |     | ldap-service |   | hadoop-namenode |   |
                  |     |  (OpenLDAP)  |   |   (WebHDFS)     |   |
                  |     +-------^------+   +--------^--------+   |
                  |             |                   |            |
+------------+    |     +-------v-------------------v----+       |
|   Client   |====|====>|         knox-gateway           |       |
| (Beeline/  |SSL |     |        (Apache Knox)           |       |
|   curl)    |8443|     +-------------------^------------+       |
+------------+    |                         |                    |
                  |                 +-------v-----+              |
                  |                 | hive-server |              |
                  |                 | (HiveServer2|              |
                  |                 +-------------+              |
                  +----------------------------------------------+
```

---

## 📋 Prerequisites

Before starting the lab, ensure you have the following installed on your host system:
1. **Docker & Docker Compose** (version 2.0+)
2. **Java JDK 11** (to compile local clients or use `keytool`)
3. **curl** (for API testing)
4. **Beeline CLI** (optional, comes with Hive/Spark installation; if not installed, you can inspect Beeline run within the Hive container)

---

## 🚀 Step 1: Initialize the Environment

Navigate to the `docker/` directory of the Day 25 project and build/start the services:

```bash
cd docker
docker-compose up -d --build
```

### Verify Containers are Running
Run `docker-compose ps` to verify that all 4 containers are online:
- `ldap-service` (Healthy)
- `knox-gateway`
- `hadoop-namenode`
- `hive-server`

---

## 🔍 Step 2: Validate LDAP Directory Setup

The OpenLDAP server is pre-loaded with groups and users defined in `docker/users.ldif`. Let's search the LDAP directory to verify the users are correctly created.

Run the following command inside the `ldap-service` container:

```bash
docker exec -it ldap-service ldapsearch -x -H ldap://localhost:389 -b "ou=people,dc=hadoop,dc=apache,dc=org" -LLL
```

You should see entry summaries for:
- `uid=admin` (Knox Administrator)
- `uid=guest` (Guest User)
- `uid=analyst` (Data Analyst)

Verify the groups:
```bash
docker exec -it ldap-service ldapsearch -x -H ldap://localhost:389 -b "ou=groups,dc=hadoop,dc=apache,dc=org" -LLL
```
- **`admin` group** has `uid=admin` as a member.
- **`analyst` group** has `uid=guest` and `uid=analyst` as members.

---

## 🛡️ Step 3: Configure Knox Topology

Review the topology file in `topologies/sandbox.xml`. It defines:
1. **Shiro LDAP Authentication:** Points to `ldap://ldap-service:389` and uses DN templates to match `uid` inside the `ou=people` organization.
2. **ACL Authorization:** Protects `WEBHDFS` and `HIVE` so that only members of the `admin` and `analyst` LDAP groups can submit requests.
3. **Backend Service URLs:** Maps Knox roles `WEBHDFS` and `HIVE` to the internal container endpoints (`http://hadoop-namenode:9870/webhdfs` and `http://hive-server:10001/cliservice`).

Because the configuration directory is mounted, Knox will automatically detect any topology file updates and hot-reload them within 5 seconds without restarting the gateway!

---

## ⚡ Step 4: Validate Knox Authentication & Perimeter Protection

Run the `verify-knox.sh` verification script located in the `scripts/` directory to test gateway behavior with different credentials.

Ensure the script has execution permissions:
```bash
chmod +x ../scripts/verify-knox.sh
../scripts/verify-knox.sh localhost 8443
```

### What this script checks:
1. **Port 8443 Check:** Confirms the Knox Gateway SSL port is listening.
2. **Authentication Denial:** Sends a query with `guest:wrong_password`. Expects an HTTP `401 Unauthorized`.
3. **Authentication Approval:** Sends a query with `guest:guestpassword`. Expects HTTP `200 OK` or `307 Temporary Redirect` (redirecting for HDFS operations).
4. **Authorization Rule:** Attempts to access the Knox Admin API (`https://localhost:8443/gateway/admin/api/v1/version`) using standard `guest` credentials. Shiro/ACL blocks this and returns a `403 Forbidden` or `401` because `guest` is not in the `admin` LDAP group.
5. **Admin Access:** Accesses the Knox Admin API using `admin:adminpassword`, which succeeds with HTTP `200 OK`.

---

## 📂 Step 5: Test Secure File Storage (WebHDFS Redirect Rewrite)

HDFS writes are a two-step HTTP operation. Let's analyze how Knox handles this by running:

```bash
chmod +x ../scripts/verify-webhdfs.sh
../scripts/verify-webhdfs.sh localhost 8443 guest guestpassword
```

### Explaining the Mechanics:
1. **The Handshake:** The client requests to create a file at `/tmp/knox-test.txt` via Knox:
   ```bash
   curl -k -u guest:guestpassword -X PUT "https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp/knox-test.txt?op=CREATE&noredirect=true"
   ```
2. **The Internal Redirect:** The Namenode receives the request from Knox and replies to Knox with a `307 Temporary Redirect` pointing to the internal DataNode host where the block should be written:
   `Location: http://hadoop-namenode:9864/webhdfs/v1/tmp/knox-test.txt?op=CREATE...`
3. **The Knox Rewrite:** Knox intercepts this redirect header. Because external clients cannot access `hadoop-namenode` on port 9864, **Knox rewrites the URL** to point back to the Knox SSL endpoint, embedding a token containing the targeted DataNode's address:
   `Location: https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp/knox-test.txt?op=CREATE&_dn=hadoop-namenode...`
4. **The Write:** The client follows this rewritten redirect, POSTs the data payload to Knox, and Knox forwards it to the internal DataNode.
5. **Verify:** The script reads back the content from HDFS, verifies it matches, and cleans up by deleting the file.

---

## 📊 Step 6: Query Hive Server 2 secure JDBC over HTTP

Next, run the Hive validation script:

```bash
chmod +x ../scripts/verify-hive.sh
../scripts/verify-hive.sh localhost 8443 guest guestpassword
```

### How Knox Secures Hive:
- Knox establishes an SSL connection with the client, protecting credentials and query payloads in transit.
- It parses user authentication from the HTTP headers, maps the LDAP groups, checks SQL access permissions, and rewrites the JDBC connection to Hive Server 2 (which runs in HTTP transport mode at port 10001).
- The validation script automatically:
  1. Downloads Knox's self-signed certificate using `openssl`.
  2. Imports it into a temporary Java KeyStore (JKS) truststore (`/tmp/knox-client-truststore.jks`).
  3. Invokes the `beeline` command with the JDBC URL containing transport configuration.

If you don't have Beeline installed on your host machine, you can run the test query directly inside the `hive-server` container using Docker:
```bash
docker exec -it hive-server beeline -u "jdbc:hive2://localhost:10001/default" -n guest -e "SHOW DATABASES;"
```
*(Note: Direct connection inside the docker network bypasses Knox. The script `verify-hive.sh` specifically routes via Knox on host port 8443 to test perimeter gateway routing).*

---

## 🧹 Step 7: Clean Up

Once you have successfully validated the gateway, shut down the cluster and clean up the docker volumes:

```bash
docker-compose down -v
```
