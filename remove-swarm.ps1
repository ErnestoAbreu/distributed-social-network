<#
Remove-swarm.ps1

Uso:
  .\remove-swarm.ps1                    # elimina el stack por defecto
  .\remove-swarm.ps1 -StackName otro    # elimina otro stack
  .\remove-swarm.ps1 -LeaveSwarm        # elimina el stack y abandona el Swarm

Opciones:
  -StackName   Nombre del stack (default: distributed_social)
  -LeaveSwarm  Si se especifica, después de eliminar el stack el nodo abandona el Swarm
#>

param(
    [string]$StackName = "distributed_social",
    [switch]$LeaveSwarm
)

function ExitWithError($msg, $code = 1) {
    Write-Error $msg
    exit $code
}

Write-Host "Comprobando Docker..."
try { docker version --format '{{.Server.Version}}' | Out-Null } catch { ExitWithError "Docker no está disponible." }

Write-Host "Eliminando stack '$StackName'..."
docker stack rm $StackName
if ($LASTEXITCODE -ne 0) {
    Write-Warning "docker stack rm devolvió un error o el stack no existía."
} else {
    Write-Host "Orden de eliminación enviada."
}

# Esperar a que servicios se eliminen
Start-Sleep -Seconds 3

if ($LeaveSwarm) {
    Write-Host "Abandonando Swarm en este nodo..."
    docker swarm leave --force
    if ($LASTEXITCODE -ne 0) { Write-Warning "docker swarm leave devolvió un error." } else { Write-Host "Swarm abandonado." }
}

Write-Host "Operación completada. Comprueba con 'docker stack ls' y 'docker node ls'."
