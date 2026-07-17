# References & Further Reading: Apache Knox Security Gateway

Below is a curated collection of links, official manuals, specifications, and design documents for deep-diving into perimeter security and Apache Knox.

---

## 📚 Official Documentation & Codebases

1. **Apache Knox User & Admin Guide**
   - [Apache Knox Official Website](https://knox.apache.org/)
   - [Knox Gateway 1.6.x Guide](https://knox.apache.org/books/knox-1-6-0/user-guide.html) (Detailed guide on services and providers).

2. **Apache Knox GitHub Repository**
   - [Official Knox Code Mirror](https://github.com/apache/knox)
   - [Knox Wiki & Developer Guide](https://cwiki.apache.org/confluence/display/KNOX/Index)

3. **Apache Shiro Security Framework**
   - [Apache Shiro Documentation](https://shiro.apache.org/) (Used by Knox ShiroProvider for authentication, LDAP realms, and session management).

---

## 🔒 Security Standards & RFC Specifications

1. **JSON Web Token (JWT)**
   - [RFC 7519: JSON Web Token Specification](https://datatracker.ietf.org/doc/html/rfc7519) (Underlying standard for KnoxSSO session tokens).
   - [JWT.io Debugger](https://jwt.io/) (Useful tool for decoding and inspecting Knox SSO tokens).

2. **OAuth 2.0 & OpenID Connect (OIDC)**
   - [RFC 6749: The OAuth 2.0 Authorization Framework](https://datatracker.ietf.org/doc/html/rfc6749)
   - [OpenID Connect Core 1.0 Specification](https://openid.net/specs/openid-connect-core-1_0.html)

3. **Kerberos Security & SPNEGO**
   - [RFC 4559: SPNEGO-based Kerberos Security in HTTP](https://datatracker.ietf.org/doc/html/rfc4559)
   - [Hadoop Kerberos Security Guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html)

---

## 📁 Enterprise Integration Guides

1. **LDAP & Active Directory Integrations**
   - [Microsoft AD LDAP Schema reference](https://learn.microsoft.com/en-us/windows/win32/adschema/active-directory-schema) (Helpful for designing group lookups and mapping member attributes).
   - [OpenLDAP Software Administrator's Guide](https://www.openldap.org/doc/admin26/)

2. **Knox Custom Service Definitions**
   - [Knox Custom Service Definition Tutorial](https://knox.apache.org/books/knox-1-6-0/dev-guide.html#Service+Definitions) (Learn how to write custom XML rules to proxy new web interfaces or REST endpoints not supported out-of-the-box).
