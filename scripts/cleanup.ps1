# Script PowerShell para limpiar todos los contenedores y redes

Write-Host "Limpiando despliegue..." -ForegroundColor Cyan
Write-Host ""

Write-Host "Deteniendo y eliminando contenedores..." -ForegroundColor Yellow
docker rm -f node-1 node-2 node-3 client-1 client-2 client-3 router
docker network rm social-network

Write-Host ""
Write-Host "Limpieza completada" -ForegroundColor Green
