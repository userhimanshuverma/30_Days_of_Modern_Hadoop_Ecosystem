#!/bin/bash
set -e

KEYTAB_DIR="/etc/security/keytabs"
mkdir -p "$KEYTAB_DIR"

# Write basic kdc.conf
cat <<EOF > /etc/krb5kdc/kdc.conf
[kdcdefaults]
    kdc_ports = 88,750

[realms]
    HADOOP.LOCAL = {
        database_name = /var/lib/krb5kdc/principal
        admin_keytab = /etc/krb5kdc/kadm5.keytab
        acl_file = /etc/krb5kdc/kadm5.acl
        key_stash_file = /etc/krb5kdc/stash
        max_life = 24h 0m 0s
        max_renewable_life = 7d 0h 0m 0s
        master_key_type = aes256-cts
        supported_enctypes = aes256-cts:normal aes128-cts:normal
    }
EOF

# Write local krb5.conf for database initialization
cat <<EOF > /etc/krb5.conf
[libdefaults]
    default_realm = HADOOP.LOCAL
    dns_lookup_realm = false
    dns_lookup_kdc = false
    rdns = false

[realms]
    HADOOP.LOCAL = {
        kdc = localhost:88
        admin_server = localhost:749
    }
EOF

# Initialize KDC database
if [ ! -f /var/lib/krb5kdc/principal ]; then
    echo "Initializing Kerberos Database..."
    kdb5_util create -s -P hadoopkdcpass
fi

# Write admin ACL
echo "*/admin@HADOOP.LOCAL *" > /etc/krb5kdc/kadm5.acl

# Start KDC services
echo "Starting KDC services..."
/usr/sbin/krb5kdc
/usr/sbin/kadmind

# Generate Principals and Keytabs
echo "Generating Principals and Keytabs..."

# 1. NameNode principal + HTTP principal (SPNEGO)
kadmin.local -q "addprinc -randkey nn/namenode.hadoop.local@HADOOP.LOCAL"
kadmin.local -q "addprinc -randkey HTTP/namenode.hadoop.local@HADOOP.LOCAL"
kadmin.local -q "xst -k $KEYTAB_DIR/nn.keytab nn/namenode.hadoop.local@HADOOP.LOCAL HTTP/namenode.hadoop.local@HADOOP.LOCAL"

# 2. DataNode principal + HTTP principal
kadmin.local -q "addprinc -randkey dn/datanode.hadoop.local@HADOOP.LOCAL"
kadmin.local -q "addprinc -randkey HTTP/datanode.hadoop.local@HADOOP.LOCAL"
kadmin.local -q "xst -k $KEYTAB_DIR/dn.keytab dn/datanode.hadoop.local@HADOOP.LOCAL HTTP/datanode.hadoop.local@HADOOP.LOCAL"

# 3. Key Management Server (KMS) principal + HTTP principal
kadmin.local -q "addprinc -randkey kms/kms-server.hadoop.local@HADOOP.LOCAL"
kadmin.local -q "addprinc -randkey HTTP/kms-server.hadoop.local@HADOOP.LOCAL"
kadmin.local -q "xst -k $KEYTAB_DIR/kms.keytab kms/kms-server.hadoop.local@HADOOP.LOCAL HTTP/kms-server.hadoop.local@HADOOP.LOCAL"

# 4. Merged SPNEGO keytab for web console integrations
kadmin.local -q "xst -k $KEYTAB_DIR/spnego.keytab HTTP/namenode.hadoop.local@HADOOP.LOCAL HTTP/datanode.hadoop.local@HADOOP.LOCAL HTTP/kms-server.hadoop.local@HADOOP.LOCAL"

# 5. Client User Principals
kadmin.local -q "addprinc -randkey alice@HADOOP.LOCAL"
kadmin.local -q "xst -k $KEYTAB_DIR/alice.keytab alice@HADOOP.LOCAL"

kadmin.local -q "addprinc -randkey hdfs@HADOOP.LOCAL"
kadmin.local -q "xst -k $KEYTAB_DIR/hdfs.keytab hdfs@HADOOP.LOCAL"

# Create HTTP signature secret file for Hadoop HTTP filters
dd if=/dev/urandom of=$KEYTAB_DIR/http_secret bs=1024 count=1

# Adjust permissions on files so Hadoop containers can read them
chmod 644 $KEYTAB_DIR/*.keytab
chmod 644 $KEYTAB_DIR/http_secret

echo "Kerberos KDC initialization complete. Service running."

# Loop to keep process alive
sleep infinity
