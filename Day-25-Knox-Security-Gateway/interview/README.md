# Interview Questions: Apache Knox Security Gateway

This guide lists interview questions with detailed, enterprise-grade answers, categorized by skill level.

---

## 🟢 Beginner Level

### 1. What is Apache Knox, and why is it referred to as a "perimeter security gateway"?
**Answer:**
Apache Knox is an application gateway that provides single-point perimeter security for Apache Hadoop clusters. It acts as a reverse proxy for Hadoop REST APIs and JDBC/ODBC connections.
It is a "perimeter" gateway because it sits at the edge of the Hadoop network cluster. Instead of exposing hundreds of ports (for NameNode, ResourceManager, WebHDFS, Hive, etc.) directly to corporate users, Knox exposes a single, hardened port (typically `8443`) over HTTPS. It prevents client machines from directly interacting with internal Hadoop service daemons, significantly reducing the attack surface.

### 2. How does Apache Knox compare to Apache Ranger and Kerberos?
**Answer:**
They solve security at different layers of the Hadoop stack:
- **Kerberos** provides strong authentication *within* the cluster (service-to-service and client-to-service). It is complex to configure and requires client-side Kerberos tickets.
- **Apache Knox** provides *perimeter security* and *API gateway features* for external users. It translates simple HTTP Basic Auth, LDAP, or JWT credentials from the client into Kerberos delegation tokens internally, shielding external clients from Kerberos complexity.
- **Apache Ranger** provides fine-grained *authorization* (column-level, row-filtering) and data governance policies. Knox handles authorization at the service/API routing layer (e.g., "Can this user access WebHDFS?"), while Ranger decides "Can this user read columns A and B from table X in Hive?".

### 3. What is a "topology" in Apache Knox, and what does it contain?
**Answer:**
A topology in Apache Knox is an XML configuration file (placed in `conf/topologies/`) that defines how requests to a specific virtual cluster path are authenticated, authorized, and routed. A topology contains:
1. **Gateway Providers:** Security configurations for authentication (e.g., Shiro/LDAP, JWT), authorization (ACLs), identity assertion (mapping user groups), and HA settings.
2. **Service Mappings:** The backend URLs of the Hadoop services (like `WEBHDFS`, `HIVE`, `YARN`) that Knox should proxy for that topology.

### 4. What protocol does Knox use to communicate with external clients?
**Answer:**
Knox communicates with external clients exclusively over **HTTPS (HTTP over SSL/TLS)**. This ensures that all traffic, client credentials, and analytical payloads are encrypted in transit between the client machine and the gateway.

### 5. What is the default port for Knox, and how do you customize it?
**Answer:**
The default port is `8443`. You customize it by editing the `gateway.port` property inside the global configuration file [gateway-site.xml](file:///C:/Users/Himanshu_Verma/DELL/Personal/30_Days_of_Modern_Hadoop_Ecosystem/Day-25-Knox-Security-Gateway/configs/gateway-site.xml).

---

## 🟡 Intermediate Level

### 6. Explain how Knox handles WebHDFS file write operations (HTTP 307 Redirects) and why URL rewriting is crucial.
**Answer:**
HDFS write operations require a two-step handshake:
1. The client submits a `PUT` request to the NameNode to create a file.
2. The NameNode responds with an HTTP `307 Temporary Redirect` containing the direct URL of the DataNode where the client must upload the actual data block (e.g., `http://internal-datanode-01:9864/webhdfs/...`).

**The Problem:** The client is outside the cluster and cannot resolve or connect to `internal-datanode-01:9864`.
**Knox's Solution (URL Rewriting):**
1. Knox intercepts the `307 Temporary Redirect` response from the NameNode.
2. Knox's rewrite engine parses the `Location` header, extracts the internal DataNode host/port, and replaces it with Knox's own public gateway address:
   `https://knox-gateway:8443/gateway/sandbox/webhdfs/v1/...`
3. Knox attaches an encrypted parameter or cookie (containing the target DataNode's internal address) to the rewritten redirect URL.
4. The client follows this redirect and uploads the file content to Knox.
5. Knox decrypts the target information and proxies the payload to the internal DataNode.

### 7. Describe the Knox Provider Framework and how it handles request lifecycle.
**Answer:**
The Provider Framework defines a pipeline of filters through which every incoming HTTP request must pass. The lifecycle steps are:
1. **Pre-authentication/Authentication:** Validates client identity (e.g., checks LDAP via Apache Shiro, verifies JWT token signatures, or authenticates Kerberos SPNEGO headers).
2. **Identity Assertion:** Takes the authenticated username and applies mapping rules (e.g., mapping user names to lower-case, group lookups, or concatenating domains).
3. **Authorization:** Checks access control rules (e.g., checking if the asserted user/group is allowed to access the specific service defined in Knox ACLs or Ranger policies).
4. **Dispatch:** Rewrites the request headers/cookies and forwards the HTTP request to the designated backend service.

### 8. How do you configure Knox to authenticate users against Microsoft Active Directory?
**Answer:**
Knox integrates with Active Directory (AD) using the **ShiroProvider** in the topology file. You configure the realm to point to the AD LDAP service:
1. Define the context URL: `ldap://ad-domain-controller:389` (or `ldaps://...:636` for secure LDAP).
2. Configure a system bind DN and password that Knox uses to search AD (e.g., `cn=KnoxBindUser,ou=ServiceAccounts,dc=corp,dc=local`).
3. Set the User Search DN template to match Active Directory user principal names (UPN) or sAMAccountNames:
   `main.ldapRealm.userDnTemplate = cn={0},ou=Users,dc=corp,dc=local` or configure a custom query using Shiro's `userSearchAttributeName = sAMAccountName`.

### 9. Explain how Knox handles Kerberos authentication internally via SPNEGO delegation.
**Answer:**
Knox acts as a **Kerberos Delegation Proxy**. When an external client makes a request, they authenticate with Knox using standard authentication (e.g., LDAP username/password or JWT).
1. Knox receives and validates the simple client credentials.
2. Knox is configured with its own Kerberos keytab and service principal (e.g., `knox/gateway-host@REALM.COM`).
3. Knox logs in to the Kerberos KDC using this keytab.
4. When forwarding the request to a Kerberized Hadoop daemon (e.g. Namenode), Knox generates a Kerberos SPNEGO token or retrieves a Hadoop **delegation token** *on behalf of* the client user.
5. Knox attaches this token to the dispatched request header, enabling the internal Hadoop cluster to identify the request as coming from the authenticated user.

### 10. What is a Knox dispatch filter, and how does it differ from a rewrite filter?
**Answer:**
- **Rewrite Filter:** Operates on the *content* of the request or response (URI path, request body, headers, HTML pages). It transforms paths (e.g., rewrites internal URLs to external ones) so that links work correctly for external users.
- **Dispatch Filter:** Operates on the *network delivery* layer. It is responsible for creating the HTTP client connection, forwarding the headers, handling HTTP timeouts, managing SSL handshake with the backend daemon, and receiving the raw payload back.

---

## 🔴 Advanced Level

### 11. How do you set up High Availability (HA) for Knox in a production Hadoop cluster?
**Answer:**
Knox HA requires redundant Knox instances behind a Load Balancer (e.g. F5, HAProxy, NGINX), coupled with ZooKeeper for topology configuration synchronization.
1. **Load Balancing:** Deploy multiple identical Knox Gateway instances. Configure the load balancer to route HTTPS traffic (port 8443) using sticky sessions or round-robin with SSL session resumption.
2. **Topology Synchronization via ZooKeeper:**
   - Configure Knox's `gateway-site.xml` to enable ZooKeeper topology monitoring.
   - Point Knox instances to the ZooKeeper quorum.
   - Topologies uploaded to Knox are serialized and stored in ZooKeeper paths (`/knox/config/topologies`).
   - Every active Knox node listens to ZooKeeper events; if a topology changes, ZooKeeper fires a watcher, and all Knox gateways pull and hot-deploy the new topology configuration in lockstep.

### 12. Explain the architecture of KnoxSSO. How does it work with JWT tokens?
**Answer:**
**KnoxSSO** is a Knox capability that provides Single Sign-On (SSO) for Hadoop web interfaces (like Hue, NameNode UI, YARN UI) and REST endpoints.
1. When a user accesses a protected Hadoop UI, they are redirected to the KnoxSSO service login page (e.g., `https://knox:8443/gateway/knoxsso/api/v1/websso`).
2. The user authenticates against KnoxSSO (e.g., using LDAP credentials, SAML, or OIDC).
3. Upon successful authentication, KnoxSSO generates a cryptographically signed **JSON Web Token (JWT)**, typically named `hadoop-jwt`.
4. The token contains claims: username, group list, issue time, and expiration. KnoxSSO signs this token using the gateway's private key.
5. The token is sent to the client's browser as a secure, HTTP-only cookie.
6. For subsequent requests to other Hadoop services proxied by Knox, the browser attaches this cookie. Knox interceptors verify the JWT's digital signature using the gateway's public key. If the signature is valid and the token hasn't expired, the user is automatically authenticated without re-entering credentials.

### 13. How does Knox's URL Rewrite Engine parse and match incoming rules? Show an example rule structure.
**Answer:**
Knox uses an XML-based declarative rewrite syntax (`rewrite.xml`). It matches URLs using path templates and rewrites them using target templates.
**Example Rule:**
```xml
<rule dir="OUT" name="WEBHDFS/webhdfs/out/datanode" pattern="http://{host}:{port}/webhdfs/v1/{path=**}?{query}">
    <rewrite template="{$gateway.url}/sandbox/webhdfs/v1/{path=**}?{query}&amp;{hostport=host:port}"/>
</rule>
```
**Explanation:**
- `dir="OUT"` means this rule applies to response headers (like Location header) sent *from* Hadoop *to* the client.
- `pattern` matches any internal HTTP redirect pointing to a DataNode host/port.
- `rewrite template` transforms this URL, replacing the direct host/port with the `$gateway.url` (Knox public URL) and appending the original host/port as a query parameter (`hostport=host:port`) so Knox knows where to forward the data during the second step.

### 14. What configuration changes must be made on HiveServer2 to allow secure routing through Apache Knox?
**Answer:**
To route JDBC traffic through Knox, HiveServer2 must be switched to **HTTP transport mode** and Knox proxy configurations must be authorized on Hadoop.
1. **HiveServer2 Configurations (`hive-site.xml`):**
   ```xml
   <property>
     <name>hive.server2.transport.mode</name>
     <value>http</value>
   </property>
   <property>
     <name>hive.server2.thrift.http.path</name>
     <value>cliservice</value>
   </property>
   <property>
     <name>hive.server2.thrift.http.port</name>
     <value>10001</value>
   </property>
   ```
2. **Hadoop Core Security Proxy Configurations (`core-site.xml`):**
   Authorize Knox to act as a proxy user on behalf of other users:
   ```xml
   <property>
     <name>hadoop.proxyuser.knox.hosts</name>
     <value>knox-gateway-host.corp.com</value>
   </property>
   <property>
     <name>hadoop.proxyuser.knox.groups</name>
     <value>*</value>
   </property>
   ```

### 15. How do you harden an Apache Knox installation for production environments?
**Answer:**
1. **Disable Weak SSL Protocols:** In `gateway-site.xml`, configure `ssl.exclude.protocols` to disable SSLv3, TLS 1.0, and TLS 1.1, enforcing TLS 1.2 or TLS 1.3 only. Use strong cipher suites.
2. **Rotate Master Secret & Keystore:** Replace the default self-signed SSL certificate with a certificate signed by a trusted Enterprise Certificate Authority (CA).
3. **Use Credential Providers:** Avoid plain-text passwords in XML topology files. Use Knox's credential store command tool to encrypt LDAP bind passwords into local `.jceks` keystores.
4. **Rate Limiting:** Enforce request throttling using a servlet filter or reverse proxy (like NGINX/HAProxy) in front of Knox.
5. **Network Isolation:** Position Knox in the DMZ network interface. The internal Hadoop cluster should be in a separate, isolated VLAN reachable *only* from the Knox host's internal network card.
