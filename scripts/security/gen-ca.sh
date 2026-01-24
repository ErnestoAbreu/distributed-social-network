#!/bin/bash

# Script para generar la Autoridad de Certificación (CA)
# Este script es compartido por servidores y clientes

CERT_DIR="${1:-.}"
CERT_DIR="$CERT_DIR/certs"

# Crear directorio si no existe
mkdir -p "$CERT_DIR"

# Verificar si la CA ya existe
if [ -f "$CERT_DIR/ca.crt" ] && [ -f "$CERT_DIR/ca.key" ]; then
    echo "CA ya existe en $CERT_DIR"
    exit 0
fi

echo "--- Generando Autoridad de Certificación (CA) ---"
openssl genrsa -out "$CERT_DIR/ca.key" 2048
openssl req -x509 -new -nodes -key "$CERT_DIR/ca.key" -subj "/CN=MyGRPCA" -days 3650 -out "$CERT_DIR/ca.crt"

echo "CA generada exitosamente"
echo "ca.key: $CERT_DIR/ca.key"
echo "ca.crt: $CERT_DIR/ca.crt"
