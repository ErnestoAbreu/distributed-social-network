<#
Deploy-swarm.ps1

Uso:
  .\deploy-swarm.ps1                      # inicializa Swarm si es necesario, construye y despliega
  .\deploy-swarm.ps1 -Build               # fuerza build local de la imagen antes de desplegar
  .\deploy-swarm.ps1 -StackName my_stack  # usar otro nombre de stack
  .\deploy-swarm.ps1 -ImageName repo/name:tag -NoBuildIfImageExists

Opciones:
  -StackName   Nombre del stack (default: distributed_social)
  -Build       Fuerza `docker build` local de la imagen (recomendado si docker-stack.yml usa `build:`)
  -ImageName   Nombre de la imagen a taggear/usar (default: distributed-social-network:latest)
  -ComposeFile Archivo de stack compose (default: docker-stack.yml)

El script verifica que Docker esté disponible, inicializa Swarm si falta,
comprueba si el compose usa `build:` y recomienda usar -Build si aplica.
#>

param(
    [string]$StackName = "distributed_social",
    [switch]$Build,
    [string]$ImageName = "distributed-social-network:latest",
    [string]$ComposeFile = "docker-stack.yml"
)

function ExitWithError($msg, $code = 1) {
    Write-Error $msg
    exit $code
}

Write-Host "Comprobando si Docker está disponible..."
try {
    docker version --format '{{.Server.Version}}' | Out-Null
} catch {
    ExitWithError "Docker no está disponible o no se puede ejecutar. Asegúrate de tener Docker instalado y en ejecución."
}

# Verificar archivo de compose
if (-not (Test-Path $ComposeFile)) {
    ExitWithError "No se encontró '$ComposeFile' en el directorio actual. Asegúrate de estar en la raíz del proyecto."
}

# Comprobar estado del Swarm
$swarmState = & docker info --format '{{.Swarm.LocalNodeState}}' 2>$null
if (-not $swarmState) { $swarmState = 'inactive' }

if ($swarmState -eq 'inactive') {
    Write-Host "Swarm no inicializado. Inicializando Swarm..."
    $initOut = docker swarm init 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "docker swarm init devolvió un error:"
        Write-Host $initOut
        ExitWithError "No se pudo inicializar Docker Swarm."
    }
    Write-Host "Swarm inicializado."
} else {
    Write-Host "Docker Swarm estado: $swarmState"
}

# Comprobar si el compose-file contiene 'build:' (y avisar si no se pide build)
$composeText = Get-Content $ComposeFile -Raw
if ($composeText -match "(?m)^\s*build:\s*") {
    if (-not $Build) {
        Write-Warning "El archivo '$ComposeFile' contiene 'build:' para algún servicio. Sin -Build, el despliegue puede fallar en nodos que no pueden construir la imagen."
        Write-Host "Si quieres construir la imagen localmente, ejecuta: .\deploy-swarm.ps1 -Build"
    }
}

if ($Build) {
    Write-Host "Construyendo la imagen local: $ImageName"
    docker build -t $ImageName .
    if ($LASTEXITCODE -ne 0) { ExitWithError "Fallo en docker build." }
}

Write-Host "Desplegando stack '$StackName' usando '$ComposeFile'..."
docker stack deploy -c $ComposeFile $StackName
if ($LASTEXITCODE -ne 0) {
    ExitWithError "docker stack deploy devolvió un error." 
}

Write-Host "Stack '$StackName' enviado. Esperando unos segundos para que los servicios se creen..."
Start-Sleep -Seconds 3

Write-Host "Puedes comprobar el estado con:"
Write-Host "  docker stack ls"
Write-Host "  docker stack services $StackName"
Write-Host "  docker service ps ${StackName}_web"

Write-Host "Si el compose utiliza 'build:', recuerda que otros nodos no construirán la imagen; para clústeres multi-nodo, sube la imagen a un registry y usa 'image: repo/name:tag' en el stack."
