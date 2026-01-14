# Script PowerShell para limpiar todos los contenedores y redes

Write-Host "Limpiando despliegue..." -ForegroundColor Cyan
Write-Host ""

Write-Host "Deteniendo y eliminando contenedores..." -ForegroundColor Yellow
docker rm -f node-0 node-1 node-2 node-3 node-4 2>$null

Write-Host ""
Write-Host "Limpieza completada" -ForegroundColor Green
