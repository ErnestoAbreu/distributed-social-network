#!/bin/bash

# Script para generar certificado y desplegar cliente con TLS

CLIENT_NAME=$1

# Validar que se proporcione el nombre del cliente
if [ -z "$CLIENT_NAME" ]; then
    echo "Error: Debe proporcionar el nombre del cliente como argumento"
    echo "Uso: $0 <nombre_del_cliente>"
    echo "Ejemplo: $0 client1"
    exit 1
fi

CERT_DIR="./certs"
mkdir -p $CERT_DIR

# Generar la CA si no existe
if [ ! -f "$CERT_DIR/ca.crt" ] || [ ! -f "$CERT_DIR/ca.key" ]; then
    echo "CA no encontrada, generando..."
    ./scripts/gen-ca.sh
fi

echo "--- 1. Generando Certificado para el Cliente: $CLIENT_NAME ---"
openssl genrsa -out $CERT_DIR/client.key 2048

# Crear archivo de configuración para el certificado del cliente
cat > $CERT_DIR/client.conf <<EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no
[req_distinguished_name]
CN = $CLIENT_NAME
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = $CLIENT_NAME
DNS.2 = localhost
EOF

# Crear solicitud de firma (CSR)
openssl req -new -key $CERT_DIR/client.key -out $CERT_DIR/client.csr -config $CERT_DIR/client.conf

# Firmar el certificado del cliente con nuestra CA
openssl x509 -req -in $CERT_DIR/client.csr -CA $CERT_DIR/ca.crt -CAkey $CERT_DIR/ca.key \
-CAcreateserial -out $CERT_DIR/client.crt -days 365 -extensions v3_req -extfile $CERT_DIR/client.conf

echo "--- 2. Desplegando Cliente en Docker ---"
docker run -d \
  --name $CLIENT_NAME \
  --hostname $CLIENT_NAME \
  --network social-network \
  --network-alias socialnet_client \
  -v $(pwd)/$CERT_DIR:/etc/grpc/certs:ro \
  -e SSL_CERT_PATH=/etc/grpc/certs/client.crt \
  -e SSL_KEY_PATH=/etc/grpc/certs/client.key \
  -e CA_CERT_PATH=/etc/grpc/certs/ca.crt \
  social-client:latest

echo "¡Despliegue del cliente completado!"
echo "Los certificados están en: $CERT_DIR"
