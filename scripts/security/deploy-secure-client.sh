#!/bin/bash

# Script para generar certificado y desplegar cliente con TLS

CLIENT_NAME=$1
PORT=$2

# Validar que se proporcione el nombre del cliente
if [ -z "$CLIENT_NAME" ]; then
    echo "Error: Debe proporcionar el nombre del cliente como argumento"
    echo "Uso: $0 <nombre_del_cliente> <puerto>"
    echo "Ejemplo: $0 client1 8501"
    exit 1
fi

if [ -z "$PORT" ]; then
    echo "Error: Debe proporcionar el puerto para el cliente como segundo argumento"
    echo "Uso: $0 <nombre_del_cliente> <puerto>"
    echo "Ejemplo: $0 client1 8501"
    exit 1
fi

CERT_DIR="./certs"

# Validar que existan los certificados de la CA
if [ ! -f "$CERT_DIR/ca.crt" ] || [ ! -f "$CERT_DIR/ca.key" ]; then
    echo "Error: Los certificados de la CA no existen en $CERT_DIR"
    echo "Por favor, genere primero la CA ejecutando el script de CA"
    exit 1
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
CN = socialnet_client
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = socialnet_client
DNS.2 = $CLIENT_NAME
DNS.3 = localhost
EOF

# Crear solicitud de firma (CSR)
openssl req -new -key $CERT_DIR/client.key -out $CERT_DIR/client.csr -config $CERT_DIR/client.conf

# Firmar el certificado del cliente con nuestra CA
openssl x509 -req -in $CERT_DIR/client.csr -CA $CERT_DIR/ca.crt -CAkey $CERT_DIR/ca.key \
-CAcreateserial -out $CERT_DIR/client.crt -days 365 -extensions v3_req -extfile $CERT_DIR/client.conf

echo "--- 2. Desplegando Cliente en Docker ---"
# docker run -d \
#   --name $CLIENT_NAME \
#   --hostname $CLIENT_NAME \
#   --network social-network \
#   --network-alias socialnet_client \
#   -p $PORT:8501 \
#   -v $(pwd)/$CERT_DIR:/etc/grpc/certs:ro \
#   -e SSL_CERT_PATH=/etc/grpc/certs/client.crt \
#   -e SSL_KEY_PATH=/etc/grpc/certs/client.key \
#   -e CA_CERT_PATH=/etc/grpc/certs/ca.crt \
#   social-client:latest

docker service create -d \
  --name $CLIENT_NAME \
  --hostname $CLIENT_NAME \
  --network social-network \
  -p $PORT:8501 \
  --mount type=bind,source=$(pwd)/$CERT_DIR,target=/etc/grpc/certs,readonly \
  -e SSL_CERT_PATH=/etc/grpc/certs/client.crt \
  -e SSL_KEY_PATH=/etc/grpc/certs/client.key \
  -e CA_CERT_PATH=/etc/grpc/certs/ca.crt \
  social-client:latest

echo "¡Despliegue del cliente completado!"
echo "Los certificados están en: $CERT_DIR"
