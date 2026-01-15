### 1. Script de Despliegue Manual (Paso a Paso)

Este script asume que ya ejecutaste `docker swarm init`. Puedes copiar y pegar estos comandos en tu terminal (en la raíz del proyecto).

#### Paso A: Crear la Red Overlay

Creamos la red `social-network` con el flag `--attachable`, lo que nos permite usar `docker run` manualmente y que los contenedores se vean entre sí.

```bash
docker network create --driver overlay --attachable social-network

```

#### Paso B: Construir las Imágenes

Es crucial que el punto final del comando sea el punto `.` (directorio actual) para enviar todo el contexto de archivos.

```bash
# Construir imagen del Servidor
docker build -f Dockerfile.server -t social-server:latest .

# Construir imagen del Cliente
docker build -f Dockerfile.client -t social-client:latest .

```

#### Paso C: Desplegar el Anillo de Servidores (Backend)

Lanzaremos 3 nodos. Usaremos `--network-alias socialnet_server` en todos. Esto crea un DNS interno estilo "Load Balancer": cuando el cliente pregunte por `socialnet_server`, Docker le dará la IP de cualquiera de los nodos vivos.

**Nodo 1 (Seed / Bootstrap):**

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

> Al tener todos el mismo `--network-alias`, el `discoverer.py` en el cliente hará `nslookup socialnet_server` y recibirá las 3 IPs. Tu código del servidor usará el `--hostname` (node-1, node-2) para generar su ID de Chord consistentemente.

#### Paso D: Desplegar el Cliente (Frontend)

El cliente se une a la misma red. Mapeamos el puerto 8501 para que puedas verlo en tu navegador.

```bash
docker run -d \
  --name client-1 \
  --network social-network \
  -p 8501:8501 \
  -e SERVER_HOST=socialnet_server \
  -e SERVER_PORT=50000 \
  social-client:latest

```

```bash
docker run -d \
  --name client-2 \
  --network social-network \
  -p 8502:8501 \
  -e SERVER_HOST=socialnet_server \
  -e SERVER_PORT=50000 \
  social-client:latest

```

```bash
docker run -d \
  --name client-3 \
  --network social-network \
  -p 8503:8501 \
  -e SERVER_HOST=socialnet_server \
  -e SERVER_PORT=50000 \
  social-client:latest

```

---

### 2. Verificación y Limpieza

**Cómo probar:**

1. Abre tu navegador en `http://localhost:8501`.
2. Deberías ver la interfaz de Login.
3. Si intentas registrarte, el cliente buscará `socialnet_server`, encontrará uno de los nodos (node-1, 2 o 3) y enviará la petición gRPC.

**Comandos útiles para debug:**

* Ver logs del nodo 1: `docker logs -f node-1`
* Ver si se ven en la red: `docker network inspect social-network`

**Cómo borrar todo para volver a empezar:**

```bash
docker rm -f node-1 node-2 node-3 social-client
docker network rm social-network

```

### Chequear estado de puertos desde el cliente

Auth

```bash
docker exec social-client python3 -c "import socket; s = socket.socket(); s.settimeout(2); print('PUERTO ABIERTO' if s.connect_ex(('10.0.1.2', 50000)) == 0 else 'PUERTO CERRADO')"
```

Post:

```bash
docker exec social-client python3 -c "import socket; s = socket.socket(); s.settimeout(2); print('PUERTO ABIERTO' if s.connect_ex(('10.0.1.2', 50001)) == 0 else 'PUERTO CERRADO')"
```

Relations:

```bash
docker exec social-client python3 -c "import socket; s = socket.socket(); s.settimeout(2); print('PUERTO ABIERTO' if s.connect_ex(('10.0.1.2', 50002)) == 0 else 'PUERTO CERRADO')"
```

### Estructura de carpetas y archivos

client
--client
----auth.py
----discoverer.py
----file_cache.py
----posts.py
----relations.py
--main.py
--__init__.py
--requirements.txt
proto
--auth.proto
--models.proto
--posts.proto
--relations.proto
protos
--auth_pb2_grpc.py
--auth_pb2.py
--models_pb2_grpc.py
--models_pb2.py
--posts_pb2_grpc.py
--posts_pb2.py
--relations_pb2_grpc.py
--relations_pb2.py
scripts
--gen_protos.sh
server
--server
----chord
------chord_db.py
------chord_discoverer.py
------chord_elector.py
------chord_list.py
------chord_replicator.py
------chord_timer.py
------constants.py
------finger_table.py
------node.py
------utils.py
----auth.py
----config.py
----posts.py
----relations.py
----utils.py
--main.py
--requirements.txt