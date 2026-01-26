### 1. Script de Despliegue Manual (Paso a Paso)

Este script asume que ya ejecutaste `docker swarm init`. Puedes copiar y pegar estos comandos en tu terminal (en la raíz del proyecto).

#### Paso A: Crear la Red Overlay

Creamos la red `social-network` con el flag `--attachable`, lo que nos permite usar `docker run` manualmente y que los contenedores se vean entre sí.

```bash
docker network create --driver overlay --attachable social-network

```

#### Paso B: Construir las Imágenes

Es crucial que el punto final del comando sea el punto `.` (directorio actual) para enviar todo el contexto de archivos.

##### Construir imagen del Servidor
```bash
docker build -f Dockerfile.server -t social-server:latest .
```

##### Construir imagen del Cliente
```bash
docker build -f Dockerfile.client -t social-client:latest .
```

##### Construir imagen del Router
```bash
docker build -f Dockerfile.router -t social-router:latest .
```

#### Paso C: Desplegar el Anillo de Servidores (Backend)

Lanzaremos 3 nodos. Usaremos `--network-alias socialnet_server` en todos. Esto crea un DNS interno estilo "Load Balancer": cuando el cliente pregunte por `socialnet_server`, Docker le dará la IP de cualquiera de los nodos vivos.

> **Nota sobre TLS**: Por defecto, los nodos se ejecutan sin TLS (`USE_TLS=false`). Para habilitar TLS con generación automática de certificados, consulta la [documentación de seguridad](README.md#3-configuración-de-seguridad-opcional).

**Nodo 1:**

```bash
docker run -d \
  --name node-1 \
  --hostname node-1 \
  --network social-network \
  --network-alias socialnet_server \
  social-server:latest

```

**Nodo 2:**

```bash
docker run -d \
  --name node-2 \
  --hostname node-2 \
  --network social-network \
  --network-alias socialnet_server \
  social-server:latest

```

**Nodo 3:**

```bash
docker run -d \
  --name node-3 \
  --hostname node-3 \
  --network social-network \
  --network-alias socialnet_server \
  social-server:latest

```

#### Paso D: Desplegar el Cliente (Frontend)

El cliente se une a la misma red. Mapeamos el puerto 8501 para que se pueda ver desde el navegador.

```bash
docker run -d \
  --name client-1 \
  --network social-network \
  --network-alias socialnet_client \
  -p 8501:8501 \
  social-client:latest

```

```bash
docker run -d \
  --name client-2 \
  --network social-network \
  --network-alias socialnet_client \
  -p 8502:8501 \
  social-client:latest

```

```bash
docker run -d \
  --name client-3 \
  --network social-network \
  --network-alias socialnet_client \
  -p 8503:8501 \
  social-client:latest

```

#### Paso E: Desplegar el Router (Recomendado)

El router proporciona un punto de acceso único que automáticamente descubre los clientes disponibles y maneja el failover. Solo se necesita acceder al router en el puerto 8080, y él se encargará de redirigir al cliente activo.

```bash
docker run -d \
  --name router \
  --network social-network \
  -p 8080:8080 \
  social-router:latest

```

**Ventajas del Router:**
- **Punto de acceso único**: Accede siempre a `http://localhost:8080`
- **Failover automático**: Si un cliente falla, el router cambia automáticamente a otro cliente disponible
- **Monitoreo continuo**: Nginx maneja la proxificación y WebSockets automáticamente

---

### 2. Configuración de seguridad (Opcional)

El sistema soporta comunicación segura mediante TLS con autenticación mutua entre cliente - servidor y servidor - servidor. Los certificados de los nodos se generan automáticamente en tiempo de ejecución.

#### Paso 1: Generar la Autoridad Certificadora (CA)

```bash
./scripts/security/gen-ca.sh
```

**Paso 2: Desplegar con TLS habilitado**

##### Servidor con TLS  

```bash
docker run -d \
  --name node-1 \
  --hostname node-1 \
  --network social-network \
  --network-alias socialnet_server \
  -v $(pwd)/certs:/etc/app/certs:ro \
  -e USE_TLS=true \
  -e CA_CERT_PATH=/app/certs/ca.crt \
  -e CA_KEY_PATH=/app/certs/ca.key \
  social-server:latest
```

```bash
docker run -d \
  --name node-2 \
  --hostname node-2 \
  --network social-network \
  --network-alias socialnet_server \
  -v $(pwd)/certs:/etc/app/certs:ro \
  -e USE_TLS=true \
  -e CA_CERT_PATH=/app/certs/ca.crt \
  -e CA_KEY_PATH=/app/certs/ca.key \
  social-server:latest
```

```bash
docker run -d \
  --name node-3 \
  --hostname node-3 \
  --network social-network \
  --network-alias socialnet_server \
  -v $(pwd)/certs:/etc/app/certs:ro \
  -e USE_TLS=true \
  -e CA_CERT_PATH=/app/certs/ca.crt \
  -e CA_KEY_PATH=/app/certs/ca.key \
  social-server:latest
```

##### Cliente con TLS  


```bash
docker run -d \
  --name client-1 \
  --hostname client-1 \
  --network social-network \
  --network-alias socialnet_client \
  -p 8501:8501 \
  -v $(pwd)/certs:/etc/app/certs:ro \
  -e USE_TLS=true \
  -e CA_CERT_PATH=/app/certs/ca.crt \
  -e CA_KEY_PATH=/app/certs/ca.key \
  social-client:latest
```

```bash
docker run -d \
  --name client-2 \
  --hostname client-2 \
  --network social-network \
  --network-alias socialnet_client \
  -p 8502:8501 \
  -v $(pwd)/certs:/etc/app/certs:ro \
  -e USE_TLS=true \
  -e CA_CERT_PATH=/app/certs/ca.crt \
  -e CA_KEY_PATH=/app/certs/ca.key \
  social-client:latest
```

```bash
docker run -d \
  --name client-3 \
  --hostname client-3 \
  --network social-network \
  --network-alias socialnet_client \
  -p 8503:8501 \
  -v $(pwd)/certs:/etc/app/certs:ro \
  -e USE_TLS=true \
  -e CA_CERT_PATH=/app/certs/ca.crt \
  -e CA_KEY_PATH=/app/certs/ca.key \
  social-client:latest
```

---

### 3. Verificación y Limpieza

**Cómo probar:**

**Con Router (Recomendado):**
1. Abre tu navegador en `http://localhost:8080`
2. El router te redirigirá automáticamente al cliente activo
3. Si un cliente falla, el router cambiará automáticamente a otro cliente disponible

**Sin Router (Acceso directo):**
1. Abre tu navegador en `http://localhost:850x`, según el cliente.
2. Deberías ver la interfaz de Login.
3. Si intentas registrarte, el cliente buscará `socialnet_server`, encontrará uno de los nodos (node-1, 2 o 3) y enviará la petición gRPC.

**Comandos útiles para debug:**

* Ver logs del nodo 1: `docker logs -f node-1`
* Ver logs del router: `docker logs -f router`
* Ver logs del cliente 1: `docker logs -f client-1`
* Ver si se ven en la red: `docker network inspect social-network`

**Cómo borrar todo para volver a empezar:**

```bash
docker rm -f node-1 node-2 node-3 client-1 client-2 client-3 router
docker network rm social-network
```