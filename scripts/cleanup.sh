#!/bin/bash

echo -e "\033[0;36mLimpiando despliegue...\033[0m"
echo ""

echo -e "\033[0;33mDeteniendo y eliminando contenedores...\033[0m"
docker rm -f node-1
# docker rm -f client-1
docker service rm client-1
# docker network rm social-network

# echo -e "\033[0;33mEliminando certificados...\033[0m"
# ./scripts/security/clean-certs.sh

echo ""
echo -e "\033[0;32mLimpieza completada\033[0m"
