# Script PowerShell para desplegar un anillo Chord con múltiples nodos en Docker

Write-Host "Desplegando Red Social Distribuida con Chord" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan

# Paso 1: Crear red overlay
Write-Host ""
Write-Host "Paso 1: Creando red overlay 'social-network'..." -ForegroundColor Yellow
docker network create --driver overlay --attachable social-network 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Red ya existe, continuando..." -ForegroundColor DarkYellow
}

# Paso 2: Construir imágenes
Write-Host ""
Write-Host "Paso 2: Construyendo imágenes Docker..." -ForegroundColor Yellow
docker build -f Dockerfile.server -t social-server:latest .
Write-Host "Imagen del servidor construida" -ForegroundColor Green

docker build -f Dockerfile.client -t social-client:latest .
Write-Host "Imagen del cliente construida" -ForegroundColor Green

# docker build -f Dockerfile.router -t social-router:latest .
# Write-Host "Imagen del router construida" -ForegroundColor Green


# Paso 3: Desplegar nodos
Write-Host ""
Write-Host "Paso 3: Desplegando nodos de servidor..." -ForegroundColor Yellow

$nodes = 1..1
foreach ($i in $nodes) {
        Write-Host "Desplegando node-$i..." -ForegroundColor Cyan
        docker run -d `
            --name "node-$i" `
            --hostname "node-$i" `
            --network social-network `
            --network-alias socialnet_server `
            social-server:latest

        Write-Host "Esperando 5 segundos para estabilización..." -ForegroundColor Gray
        Start-Sleep -Seconds 5
}

Write-Host "Todos los nodos del anillo desplegados" -ForegroundColor Green

# Paso 4: Desplegar cliente
Write-Host ""
Write-Host "Paso 4: Desplegando clientes Streamlit..." -ForegroundColor Yellow
docker service create --name client-1 --network social-network --publish 8501:8501 social-client:latest
Write-Host "Cliente desplegado en http://localhost:8501" -ForegroundColor Green

#Paso 5: Desplegar router (opcional)
# Write-Host ""
# Write-Host "Paso 5: Desplegando router..." -ForegroundColor Yellow
# docker run -d --name router --network social-network -p 8080:8080 social-router:latest


# Resumen
Write-Host ""
Write-Host "DESPLIEGUE COMPLETADO" -ForegroundColor Green
Write-Host "========================" -ForegroundColor Green
Write-Host ""
Write-Host "Nodos desplegados:" -ForegroundColor Cyan
docker ps --filter "name=node-" --format "  * {{.Names}} - {{.Status}}"
Write-Host ""
Write-Host "Cliente web disponible en: http://localhost:8501" -ForegroundColor Cyan
Write-Host ""
Write-Host "Comandos utiles:" -ForegroundColor Yellow
Write-Host " * Ver logs de un nodo:    docker logs -f node-1" -ForegroundColor White
Write-Host " * Ver red:                docker network inspect social-network" -ForegroundColor White
Write-Host " * Limpiar todo:           .\scripts\cleanup.ps1" -ForegroundColor White
Write-Host ""
