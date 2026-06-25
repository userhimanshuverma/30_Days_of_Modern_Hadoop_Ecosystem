# Day 4 References & Deep Reads: HDFS Security & Encryption

This document compiles official resources, architectural papers, and specifications on securing Hadoop clusters.

---

## 1. Apache Hadoop Official Documentation
- **[Hadoop Secure Mode](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html)**: The official operational guide for configuring Kerberos authentication, SPNEGO HTTP authentication, secure DataNodes, proxy users, and SSL.
- **[Hadoop KMS (Key Management Server)](https://hadoop.apache.org/docs/stable/hadoop-kms/index.html)**: Complete REST API specification and XML properties reference for KMS.
- **[Transparent Data Encryption (TDE) in HDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/TransparentEncryption.html)**: Mechanics of HDFS Encryption Zones, key provisioning, and clients integration.
- **[HDFS Permissions and Extended ACLs](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html)**: Syntax and evaluation rules for standard POSIX modes and access control lists.

---

## 2. MIT Kerberos Documentation
- **[MIT Kerberos Consortium Documentation](https://web.mit.edu/kerberos/krb5-latest/doc/)**: Detailed manuals on Kerberos configuration (`krb5.conf`), realm setup (`kdc.conf`), administration, and GSS-API/SPNEGO.
- **[RFC 4120 - The Kerberos Network Authentication Service (V5)](https://datatracker.ietf.org/doc/html/rfc4120)**: The official Internet Engineering Task Force (IETF) specification detailing ticket exchanges, message formats, and security assertions.

---

## 3. Security Governance Engines
- **[Apache Ranger](https://ranger.apache.org/)**: Policy repository, audit logger, and plugin hooks architecture for centralized security governance.
- **[Apache Knox Gateway](https://knox.apache.org/)**: Secure HTTP/REST API reverse proxy designed to restrict direct exposure of internal cluster NameNode/KMS nodes.

---

## 4. Whitepapers & Design Notes
- **[Adding Security to Apache Hadoop (O'Malley et al.)](https://www.usenix.org/legacy/event/usenix10/tech/full_papers/omalley.pdf)**: The primary research paper detailing why and how Kerberos authentication was retrofitted into Hadoop 0.20/1.x, establishing the framework for Delegation Tokens and Block Access Tokens.
- **[Security and Compliance in Enterprise Data Lakes](https://www.oreilly.com/library/view/architecting-data-lakes/9781491931851/)**: Design patterns for GDPR and HIPAA compliance, encryption boundaries, and unified authorization layers.
