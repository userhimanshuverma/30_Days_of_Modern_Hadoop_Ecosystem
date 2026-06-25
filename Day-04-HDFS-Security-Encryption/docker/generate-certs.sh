#!/bin/bash
set -e

SSL_DIR="/var/ssl"
mkdir -p "$SSL_DIR"

cd "$SSL_DIR"

PASS_STORE="hadoopstorepass"
PASS_KEY="hadoopkeypass"
PASS_TRUST="hadooptrustpass"

if [ -f keystore.jks ] && [ -f truststore.jks ]; then
    echo "Certificates already exist. Skipping generation."
    exit 0
fi

echo "Generating Certificate Authority (CA)..."
openssl req -new -x509 -keyout ca-key.pem -out ca-cert.pem -days 365 -nodes \
  -subj "/C=US/ST=State/L=City/O=HadoopLocal/OU=IT/CN=hadoop-ca"

echo "Generating Server KeyStore..."
keytool -keystore keystore.jks -alias localhost -validity 365 -genkey -keyalg RSA \
  -storepass "$PASS_STORE" -keypass "$PASS_KEY" \
  -dname "CN=*.hadoop.local, OU=IT, O=HadoopLocal, L=City, S=State, C=US"

echo "Exporting Cert from Keystore..."
keytool -keystore keystore.jks -alias localhost -certreq -file cert-file.pem \
  -storepass "$PASS_STORE"

echo "Signing Server Cert with CA..."
openssl x509 -req -CA ca-cert.pem -CAkey ca-key.pem -in cert-file.pem -out cert-signed.pem \
  -days 365 -CAcreateserial

echo "Importing CA into Keystore..."
keytool -keystore keystore.jks -alias CARoot -import -noprompt -file ca-cert.pem \
  -storepass "$PASS_STORE"

echo "Importing Signed Cert into Keystore..."
keytool -keystore keystore.jks -alias localhost -import -noprompt -file cert-signed.pem \
  -storepass "$PASS_STORE"

echo "Generating Client TrustStore..."
keytool -keystore truststore.jks -alias CARoot -import -noprompt -file ca-cert.pem \
  -storepass "$PASS_TRUST"

# Adjust permissions so they can be read by Hadoop processes
chmod 644 keystore.jks truststore.jks ca-cert.pem ca-key.pem
echo "SSL/TLS Keystore & Truststore generation completed!"
