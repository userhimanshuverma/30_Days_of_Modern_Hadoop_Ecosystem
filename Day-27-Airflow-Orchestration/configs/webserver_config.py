# configs/webserver_config.py
# Production Web Server Configuration for Apache Airflow RBAC
# Sets up security managers, authentication filters, and theme overrides.

import os
from flask_appbuilder.security.manager import AUTH_DB, AUTH_LDAP

# Base directory for Flask AppBuilder database (if using local SQL authentication)
basedir = os.path.abspath(os.path.dirname(__file__))

# ----------------------------------------------------
# AUTHENTICATION CONFIGURATION
# ----------------------------------------------------
# Choose authentication type: AUTH_DB (Database) or AUTH_LDAP (LDAP/Active Directory)
# In production, AUTH_LDAP is standard.
AUTH_TYPE = AUTH_DB

# If using LDAP authentication, uncomment and configure below:
# AUTH_TYPE = AUTH_LDAP
# AUTH_LDAP_SERVER = "ldap://ldap.enterprise.com:389"
# AUTH_LDAP_USE_TLS = True
# AUTH_LDAP_BIND_USER = "cn=service_account,ou=users,dc=enterprise,dc=com"
# AUTH_LDAP_BIND_PASSWORD = "service_account_secure_password"
# AUTH_LDAP_SEARCH = "ou=users,dc=enterprise,dc=com"
# AUTH_LDAP_UID_FIELD = "sAMAccountName" # Or 'uid' for OpenLDAP
# AUTH_LDAP_GROUP_FIELD = "memberOf"
# AUTH_LDAP_SEARCH_FILTER = "(objectClass=person)"

# Group-to-Role mapping for LDAP
# AUTH_ROLES_MAPPING = {
#     "cn=airflow_admins,ou=groups,dc=enterprise,dc=com": ["Admin"],
#     "cn=airflow_ops,ou=groups,dc=enterprise,dc=com": ["Op"],
#     "cn=airflow_users,ou=groups,dc=enterprise,dc=com": ["User"],
#     "cn=airflow_viewers,ou=groups,dc=enterprise,dc=com": ["Viewer"],
# }

# Registration settings
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer" # Default role for new self-registered users

# ----------------------------------------------------
# SECURITY SETTINGS
# ----------------------------------------------------
# Require users to log out before logging in again
# AUTH_ROLE_ADMIN = 'Admin'
# AUTH_ROLE_PUBLIC = 'Public'

# Flask-WTF CSRF Protection settings
WTF_CSRF_ENABLED = True
WTF_CSRF_TIME_LIMIT = 3600 # 1 hour

# ----------------------------------------------------
# THEME AND UI CUSTOMIZATION
# ----------------------------------------------------
# Custom title and branding for the Airflow Webserver console
APP_NAME = "Hadoop Ecosystem Orchestration Portal (Airflow)"
APP_ICON = "/static/pin_32.png"

# Flask AppBuilder built-in themes:
# "amelia", "cerulean", "cosmo", "cyborg", "flatly", "journal", "readable", "simplex", 
# "slate", "spacelab", "united", "yeti"
APP_THEME = "slate.css" # Modern dark-slate dashboard theme
