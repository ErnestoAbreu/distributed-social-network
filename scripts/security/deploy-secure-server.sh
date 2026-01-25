#!/bin/bash

# Script para generar certificado y desplegar servidor con TLS

NODE_NAME=$1

# Validar que se proporcione el nombre del nodo
if [ -z "$NODE_NAME" ]; then
    echo "Error: Debe proporcionar el nombre del nodo como argumento"
    echo "Uso: $0 <nombre_del_nodo>"
    echo "Ejemplo: $0 server1"
    exit 1
fi

CERT_DIR="./certs"
mkdir -p $CERT_DIR

# Generar la CA si no existe
if [ ! -f "$CERT_DIR/ca.crt" ] || [ ! -f "$CERT_DIR/ca.key" ]; then
    echo "CA no encontrada, generando..."
    ./scripts/gen-ca.sh
fi

echo "--- 1. Generando Certificado para el Nodo: $NODE_NAME ---"
openssl genrsa -out $CERT_DIR/server.key 2048

# Crear archivo de configuración para que el certificado sea válido para el nombre del host de Docker
cat > $CERT_DIR/server.conf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no
[req_distinguished_name]
CN = socialnet_server
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = socialnet_server
DNS.2 = $NODE_NAME
DNS.3 = localhost
EOF

# Crear solicitud de firma (CSR)
openssl req -new -key $CERT_DIR/server.key -out $CERT_DIR/server.csr -config $CERT_DIR/server.conf

# Firmar el certificado del nodo con nuestra CA
openssl x509 -req -in $CERT_DIR/server.csr -CA $CERT_DIR/ca.crt -CAkey $CERT_DIR/ca.key \
-CAcreateserial -out $CERT_DIR/server.crt -days 365 -extensions v3_req -extfile $CERT_DIR/server.conf

echo "--- 2. Desplegando Nodo en Docker ---"
docker run -d \
  --name $NODE_NAME \
  --hostname $NODE_NAME \
  --network social-network \
  --network-alias socialnet_server \
  -v $(pwd)/$CERT_DIR:/etc/grpc/certs:ro \
  -e SSL_CERT_PATH=/etc/grpc/certs/server.crt \
  -e SSL_KEY_PATH=/etc/grpc/certs/server.key \
  -e CA_CERT_PATH=/etc/grpc/certs/ca.crt \
  social-server:latest

echo "¡Despliegue completado!"
echo "Los certificados están en: $CERT_DIR"