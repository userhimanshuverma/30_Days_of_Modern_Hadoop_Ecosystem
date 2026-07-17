# Production Troubleshooting Playbook: Apache Knox Gateway

This guide covers common errors, root causes, diagnostic logs, and resolutions when deploying and maintaining Apache Knox in production environments.

---

## 🗺️ Troubleshooting Flowchart

```
                 +---------------------------+
                 |   Client Request Fails    |
                 +-------------+-------------+
                               |
            +------------------+------------------+
            |                                     |
    [ Network / Port ]                   [ HTTP Status Code ]
            |                                     |
            v                                     v
   - Check bind address                  +--------+--------+
   - Verify Docker networks              |                 |
   - Check SSL cert Trust               401               403
                                         |                 |
                                         v                 v
                                  - LDAP Search DN  - Check Topology ACLs
                                  - Shiro Realm     - Verify LDAP Group
                                  - Master secret   - Match roles exactly
```

---

## 🔍 Diagnostic Checklist: Where are the logs?

Before debugging, know where Apache Knox stores its diagnostic outputs:
- **Service Deployment & Startup Logs:** `/opt/knox/logs/gateway.log` (Essential for fixing start-up crashes and parsing XML syntax errors in topologies).
- **Audit Logs:** `/opt/knox/logs/gateway-audit.log` (Tracks all incoming requests, user mappings, auth decisions, and dispatch targets).
- **LDAP Access Logs:** Inside the LDAP container/host (typically `/var/log/ldap` or stdout).

---

## 🚨 Scenario 1: Knox Gateway Fails to Start (Keystore / SSL Errors)

### Symptom
Knox service crashes immediately upon startup or outputs the following log:
```
java.io.IOException: Keystore was tampered with, or password was incorrect
    at sun.security.provider.JavaKeyStore.engineLoad(JavaKeyStore.java:780)
    ...
```

### Root Cause
1. The master secret password used to run the gateway does not match the master secret used to encrypt the existing `gateway.jks` keystore inside `data/security/keystores/`.
2. The keystore permissions are restricted, preventing the `knox` user from reading the directory.

### Remediation
- **If master secret was lost:** Delete the local keystores and regenerate them with the current secret:
  ```bash
  rm -rf /opt/knox/data/security/keystores/*
  /opt/knox/bin/knox-cli.sh create-master --master "your_new_secret"
  /opt/knox/bin/knox-cli.sh create-cert --hostname "your-knox-hostname"
  ```
- **Fix permissions:** Ensure the keystores are owned by `knox:knox` and have `600` permissions:
  ```bash
  chown -R knox:knox /opt/knox/data/security/
  chmod 600 /opt/knox/data/security/keystores/*
  ```

---

## 🔒 Scenario 2: SSL Handshake Failure (PKIX Path Building Failed)

### Symptom
Client commands (e.g. Curl or Beeline) crash with:
```
curl: (60) SSL certificate problem: self signed certificate in certificate chain
# OR
javax.net.ssl.SSLHandshakeException: sun.security.validator.ValidatorException: PKIX path building failed: unable to find valid certification path to requested target
```

### Root Cause
The client does not trust Knox Gateway's SSL certificate because it is either self-signed or signed by an internal CA not present in the client's local Java truststore (`cacerts`).

### Remediation
1. **Fetch the Certificate:**
   ```bash
   openssl s_client -connect <knox-host>:8443 -showcerts </dev/null 2>/dev/null | openssl x509 -outform PEM > /tmp/knox.crt
   ```
2. **Import into Client Truststore:**
   ```bash
   keytool -importcert -noprompt \
     -alias knox-gateway \
     -file /tmp/knox.crt \
     -keystore /tmp/knox-truststore.jks \
     -storepass changeit
   ```
3. **Reference in client connection string:**
   - **Beeline:** `jdbc:hive2://<host>:8443/;ssl=true;sslTrustStore=/tmp/knox-truststore.jks;trustStorePassword=changeit;transportMode=http;httpPath=gateway/sandbox/hive`
   - **Curl:** Use `--cacert /tmp/knox.crt` (or `-k` for testing only, never in production).

---

## 🔑 Scenario 3: LDAP Authentication Failure (401 Unauthorized)

### Symptom
Clients authenticate with LDAP credentials but get:
```
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Basic realm="device-properties"
```
The `/opt/knox/logs/gateway.log` shows:
```
javax.naming.AuthenticationException: [LDAP: error code 49 - Invalid Credentials]
```

### Root Cause
1. The **System Bind User** (`main.ldapRealm.systemUsername`) password in the topology file is incorrect.
2. The User DN template (`main.ldapRealm.userDnTemplate`) is misaligned with the LDAP directory path (e.g. using `uid={0},ou=people` when users are in `uid={0},ou=users`).

### Remediation
- Run `ldapsearch` using the exact bind parameters configured in Knox to isolate Shiro configuration vs LDAP directory issues:
  ```bash
  ldapsearch -x -h ldap-service -p 389 -D "cn=admin,dc=hadoop,dc=apache,dc=org" -w adminpassword -b "ou=people,dc=hadoop,dc=apache,dc=org" "(uid=guest)"
  ```
- Adjust `sandbox.xml` `userDnTemplate` to match the exact tree node structure returned by the search.

---

## 🚫 Scenario 4: Authorization Access Blocked (403 Forbidden)

### Symptom
Authentication succeeds, but the HTTP response returns `403 Forbidden`. The `gateway-audit.log` shows:
```
audit | ... | sandbox | WEBHDFS | guest | ... | access | denied |
```

### Root Cause
The user's LDAP group memberships do not match the authorized groups specified under the `AclsAuthz` provider parameter (e.g., `<param><name>WEBHDFS.acl</name><value>*;admin,analyst;*</value></param>` blocks `guest` if they are not in the `admin` or `analyst` LDAP groups).

### Remediation
1. Verify the group search base and object classes in the authentication provider configuration:
   ```xml
   <param>
       <name>main.ldapRealm.groupSearchBase</name>
       <value>ou=groups,dc=hadoop,dc=apache,dc=org</value>
   </param>
   ```
2. Run `ldapsearch` to inspect if the user dn is mapped under the group's `member` attribute:
   ```bash
   ldapsearch -x -h ldap-service -D "cn=admin,dc=hadoop,dc=apache,dc=org" -w adminpassword -b "ou=groups,dc=hadoop,dc=apache,dc=org"
   ```

---

## 🗺️ Scenario 5: Service Routing Error (404 Not Found)

### Symptom
Clients query Knox but receive `404 Not Found`.

### Root Cause
1. The service role name in the request URL does not match any service declared in the topology (e.g. requesting `/gateway/sandbox/webhdfs/v1` when the role is misspelled as `<role>WEBHDFS-NEW</role>`).
2. The service role is case-sensitive (e.g. `/gateway/sandbox/webhdfs` vs `/gateway/sandbox/WEBHDFS` mapping).
3. The Knox Gateway has failed to deploy the topology due to parsing errors.

### Remediation
- Check `/opt/knox/logs/gateway.log` for deployment status:
  ```
  Deployment of topology sandbox failed.
  org.xml.sax.SAXParseException: XML document structures must start and end within the same entity.
  ```
- Fix the XML syntax and ensure the path case matches the service definition.

---

## 🔄 Scenario 6: URL Rewriting & Dispatch Failures (Kerberos/SPNEGO)

### Symptom
Client hits WebHDFS write endpoint and gets redirected, but subsequent upload fails or drops connectivity, or Knox logs print:
```
javax.security.auth.login.LoginException: No LoginModule found for pg-client
```

### Root Cause
When Knox proxies requests to a Kerberized Hadoop cluster, it must authenticate itself as a service principal using SPNEGO. If Knox lacks a valid ticket or keytab, Hadoop rejects it.

### Remediation
1. Confirm Knox has a valid Kerberos credential cache:
   ```bash
   klist -kt /etc/security/keytabs/knox.service.keytab
   kinit -kt /etc/security/keytabs/knox.service.keytab knox/knox-gateway-host@YOUR-REALM.COM
   ```
2. Verify `/opt/knox/conf/krb5.conf` is loaded and the path is added to Java options inside `/opt/knox/bin/gateway.sh` or systemd:
   ```bash
   -Djava.security.krb5.conf=/opt/knox/conf/krb5.conf
   -Djava.security.auth.login.config=/opt/knox/conf/gateway-jaas.conf
   ```
