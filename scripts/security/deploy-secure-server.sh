#!/bin/bash

# Script para desplegar servidor con TLS

SERVER_NAME=$1

# Validar que se proporcione el nombre del nodo
if [ -z "$SERVER_NAME" ]; then
    echo "Error: Debe proporcionar el nombre del nodo como argumento"
    echo "Uso: $0 <nombre_del_nodo>"
    echo "Ejemplo: $0 server-1"
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

echo "--- Desplegando Servidor en Docker ---"
docker run -d \
  --name $SERVER_NAME \
  --hostname $SERVER_NAME \
  --network social-network \
  --network-alias socialnet_server \
  -v $(pwd)/$CERT_DIR:/etc/app/certs:ro \
  -e USE_TLS=true \
  -e CA_CERT_PATH=/app/certs/ca.crt \
  -e CA_KEY_PATH=/app/certs/ca.key \
  social-server:latest

echo "--- ¡Despliegue de servidor completado! ---"