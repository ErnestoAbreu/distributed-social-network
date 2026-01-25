#!/bin/bash

# Script para desplegar cliente con TLS

CLIENT_NAME=$1
PORT=$2

# Validar que se proporcione el nombre del cliente
if [ -z "$CLIENT_NAME" ]; then
    echo "Error: Debe proporcionar el nombre del cliente como argumento"
    echo "Uso: $0 <nombre_del_cliente> <puerto>"
    echo "Ejemplo: $0 client1 8501"
    exit 1
fi

# Validar que se proporcione el puerto
if [ -z "$PORT" ]; then
    echo "Error: Debe proporcionar el puerto para el cliente como segundo argumento"
    echo "Uso: $0 <nombre_del_cliente> <puerto>"
    echo "Ejemplo: $0 client1 8501"
    exit 1
fi

CERT_DIR="./certs"
mkdir -p $CERT_DIR

# Validar que existan los certificados de la CA
if [ ! -f "$CERT_DIR/ca.crt" ] || [ ! -f "$CERT_DIR/ca.key" ]; then
    echo "Error: Los certificados de la CA no existen en $CERT_DIR"
    echo "Por favor, genere primero la CA ejecutando el script de CA"
    exit 1
fi


echo "--- Desplegando Cliente en Docker ---"
# docker run -d \
#   --name $CLIENT_NAME \
#   --hostname $CLIENT_NAME \
#   --network social-network \
#   --network-alias socialnet_client \
#   -p $PORT:8501 \
#   -v $(pwd)/$CERT_DIR:/etc/app/certs:ro \
#   -e USE_TLS=true \
#   -e CA_CERT_PATH=/app/certs/ca.crt \
#   -e CA_KEY_PATH=/app/certs/ca.key \
#   social-client:latest

docker service create -d \
  --name $CLIENT_NAME \
  --hostname $CLIENT_NAME \
  --network social-network \
  --network-alias socialnet_client \
  -p $PORT:8501 \
  --mount type=bind,source=$(pwd)/$CERT_DIR,target=/etc/app/certs,readonly \
  -e USE_TLS=true \
  -e CA_CERT_PATH=/app/certs/ca.crt \
  -e CA_KEY_PATH=/app/certs/ca.key \
  social-client:latest

echo "--- ¡Despliegue del cliente completado! ---"
