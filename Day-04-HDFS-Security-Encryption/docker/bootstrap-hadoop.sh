#!/bin/bash
set -e

KEYTAB_DIR="/etc/security/keytabs"
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/opt/hadoop/etc/hadoop}

echo "Waiting for Kerberos keytabs..."
while [ ! -f "$KEYTAB_DIR/nn.keytab" ] || [ ! -f "$KEYTAB_DIR/dn.keytab" ] || [ ! -f "$KEYTAB_DIR/kms.keytab" ]; do
  sleep 2
done
echo "Keytabs detected!"

# Copy configuration templates to their active Hadoop paths
cp /tmp/configs/krb5.conf /etc/krb5.conf
cp /tmp/configs/core-site.xml $HADOOP_CONF_DIR/core-site.xml
cp /tmp/configs/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
cp /tmp/configs/ssl-server.xml $HADOOP_CONF_DIR/ssl-server.xml
cp /tmp/configs/ssl-client.xml $HADOOP_CONF_DIR/ssl-client.xml

if [ "$SERVICE_TYPE" = "kms" ]; then
    cp /tmp/configs/kms-site.xml $HADOOP_CONF_DIR/kms-site.xml
    cp /tmp/configs/kms-acls.xml $HADOOP_CONF_DIR/kms-acls.xml
fi

# NameNode generates certs, other containers wait for them
if [ "$SERVICE_TYPE" = "namenode" ]; then
    bash /tmp/docker/generate-certs.sh
fi

echo "Waiting for SSL/TLS certificates..."
while [ ! -f "/var/ssl/keystore.jks" ] || [ ! -f "/var/ssl/truststore.jks" ]; do
  sleep 2
done
echo "Certificates found!"

# Add the CA cert to the CentOS certificate bundle for local command-line client verification (curl, openssl)
cp /var/ssl/ca-cert.pem /etc/pki/ca-trust/source/anchors/hadoop-ca.crt
update-ca-trust

if [ "$SERVICE_TYPE" = "namenode" ]; then
    # Format NameNode if not formatted already
    if [ ! -f "/var/lib/hadoop/dfs/name/current/VERSION" ]; then
        echo "Formatting NameNode..."
        hdfs namenode -format -force -nonInteractive
    fi
    echo "Starting Secure NameNode..."
    hdfs namenode
elif [ "$SERVICE_TYPE" = "datanode" ]; then
    echo "Waiting for NameNode port 9000 to be open..."
    while ! device_ports=$(cat /proc/net/tcp | grep -i "00000000:2328"); do # 9000 in hex is 2328, but checking namenode service port is better
       # Check port using bash socket or nc
       (echo > /dev/tcp/namenode/9000) >/dev/null 2>&1 && break
       sleep 2
    done
    echo "Starting Secure DataNode..."
    # Starting secure datanode. In Hadoop HA/Security, since we have configured SASL privacy
    # and HTTP policy HTTPS_ONLY, we do not require root container execution or JSVC.
    hdfs datanode
elif [ "$SERVICE_TYPE" = "kms" ]; then
    echo "Starting Hadoop Key Management Server..."
    mkdir -p /var/lib/hadoop
    kms.sh run
elif [ "$SERVICE_TYPE" = "client" ]; then
    echo "Client container ready!"
    # Autologin with alice to verify basic ticket retrieval
    kinit -kt /etc/security/keytabs/alice.keytab alice@HADOOP.LOCAL
    # Sleep to allow active exec testing
    sleep infinity
else
    echo "Unknown service type: $SERVICE_TYPE"
    exit 1
fi
