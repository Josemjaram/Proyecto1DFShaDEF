# DFSha — sistema de archivos distribuido (Proyecto 1, Sistemas Distribuidos 2026-2)
Integrantes:
David Pabón 
Pablo Henao 
José Jaramillo  
Opción 1: cliente/servidor con servicio distribuido entre servidores (S2S) en red privada. Python 3.12 · FastAPI · Raft (PySyncObj).

```
                    ┌──────────── plano de control (metadatos, Raft 2 de 3) ────────────┐
   Cliente/CLI ───► │  Controlador c1 ◄──► Controlador c2 ◄──► Controlador c3           │
   (solo conoce las │  usuarios · namespace · versiones · locks · asignación · tokens   │
    URL de control) └───────────▲─────────────────────────────────────▲─────────────────┘
        │  1) pide manifiesto/ubicaciones     heartbeat + blockreport  │ reparación / GC
        │                                                              │
        └────── 2) datos directos (HTTPS + token acotado) ─────► Nodos de datos n1…nK
                   bloques inmutables · AES-256-GCM en reposo · SHA-256 · réplicas
```

## Arranque rápido (local, procesos separados con TLS)
```bash
pip install -r requirements.txt
python scripts/local_cluster.py up --dir /tmp/dfsha        # 3 controladores + 4 nodos; imprime los export
export DFSHA_CONTROLLERS=https://127.0.0.1:8001,https://127.0.0.1:8002,https://127.0.0.1:8003
export DFSHA_CA=/tmp/dfsha/certs/ca.pem DFSHA_CLUSTER_KEY=test-cluster-key
./dfsha-cli register --user ana --password clave123
./dfsha-cli mkdir /docs && ./dfsha-cli cd /docs
./dfsha-cli put ./video.mp4 video.mp4 && ./dfsha-cli ls -l && ./dfsha-cli get video.mp4 ./copia.mp4
```
Comandos: `register login whoami pwd cd ls [-l] stat mkdir rmdir rm put [--append|--create] get read [--offset --length] write [--append|--offset N] lock unlock cluster`.
API Python (RF3): `c.open(path,'r'|'w'|'a'|'x')` → `read/seek/write/close/abort`, `c.lock/unlock`; modificar un rango: `c.write_at(path, offset, data)` (o `c.open(path,'m')` + `modify`).

## Pruebas (cubren las 7 mínimas del enunciado)
```bash
python tests/run_tests.py            # ~6 min; escribe results/report.md y results/perf.csv
python tests/run_tests.py --only seguridad
```
| Prueba del enunciado | Función | Evidencia |
|---|---|---|
| Carga y recuperación | `t_carga_recuperacion` | 64 MiB en 16 bloques sobre 4 nodos, 3 copias, hash idéntico |
| Acceso RF3 | `t_rf3` | open/read/write/close/lock, snapshot, abortar, `expected_version` |
| Modificación por bloque (RF3) | `t_modificacion_bloque` | 1 byte → solo 1 bloque nuevo (los demás se comparten), rango que cruza bloques, snapshot anterior intacto, 423/412/416, GC libera solo los bloques reemplazados |
| Concurrencia | `t_concurrencia` | 423 al segundo escritor, carrera de 6 hilos sin mezcla, escritor zombi rechazado (fencing) |
| Replicación y fallos | `t_fallos_nodo`, `t_nodo_cae_durante_escritura` | lectura/escritura con nodo caído, detección, reparación a 3 copias, reingreso del nodo |
| Consistencia | `t_replica_retrasada` | réplica congelada durante una escritura: se publica con ≥2 copias, lectores ven la versión nueva completa, la réplica se repone |
| Seguridad | `t_seguridad` | 401/403, tokens acotados, TLS 1.3, CA desconocida rechazada, texto plano ausente en disco, bloque alterado detectado y reparado |
| Rendimiento | `t_rendimiento` | tamaño de bloque (1/4/8 MiB) y clientes (1/2/4); mediana y p95 |
| Extras | `t_escritura_interrumpida`, `t_gc_versiones`, `t_failover_controlador` | bloque parcial rechazado, huérfanos limpiados, GC, failover de líder y pérdida de mayoría |

> Los números de `results/` salieron de **1 vCPU en localhost**: sirven para validar corrección, no como mediciones de AWS. Repita `t_rendimiento` en AWS (≥5 repeticiones) y reporte esas cifras.

## Despliegue en AWS Academy (4 VMs, casi automático)
Topología: vm1-vm3 = controlador + nodo de datos (contenedores separados); vm4 = nodo de datos + cliente. Detalle, tiempos, solución de problemas y plan B manual en [`docs/GUIA_AWS.md`](docs/GUIA_AWS.md).

```bash
# una vez: credenciales del Learner Lab en ~/.aws/credentials y labsuser.pem descargada
export SSH_KEY=~/labsuser.pem SSH_USER=ec2-user AWS_REGION=us-east-1
bash deploy/aws_crear_vms.sh        # crea las 4 VMs y escribe deploy/hosts.txt
bash deploy/desplegar.sh            # secretos, certificados por VM, Docker, arranque y prueba de punta a punta
bash deploy/evidencia_aws.sh        # fallas (stop/kill/pause), seguridad, rendimiento -> carpeta evidencia/
bash deploy/aws_crear_vms.sh detener    # al terminar: ahorra presupuesto
```
- Los secretos (`DFSHA_CLUSTER_KEY`, `DFSHA_ADMIN_KEY`, `DFSHA_NODE_KEY`) son obligatorios y los genera `desplegar.sh` en `deploy/secrets.generated.env` (fuera del repositorio). `DFSHA_NODE_KEY` es distinta por nodo: las réplicas viajan en claro dentro de TLS y cada nodo vuelve a cifrar con su propia llave.
- Un certificado por VM (lo comparten el controlador y el nodo de datos de esa VM), firmado por una CA propia; `certs/ca.key` nunca sale del computador.
- Inyección de fallas: `docker stop` (parada manual: **no** se reinicia sola), `kill -9` al proceso del contenedor (caída real: Docker lo levanta por `--restart unless-stopped`; `docker kill` cuenta como parada manual y **no** se reinicia), `docker pause` (réplica desconectada), detener al líder (failover). `evidencia_aws.sh` ejecuta todo y guarda la salida.
- **Estado de verificación**: código probado en un clúster local (suite local sin fallos, ver `results/report.md`) y **ejecutado en AWS Academy el 8 de noviembre de 2026** (4 VMs t3.small, us-east-1): despliegue, prueba de punta a punta y pasos 00 a 12 sin fallos. Evidencia y cifras de rendimiento de AWS en [`evidencia/`](evidencia/) (`resumen.txt`, `aws_perf.csv`, `logs/`).
- Limitación declarada: controlador y nodo de datos comparten VM (reduce el aislamiento de fallas, no el software).

### Agregar un nodo de datos (sin tocar los demás)
1. `python scripts/gen_certs.py certs --node vm5=10.0.1.25` (reutiliza la CA existente; no cambia los certificados de los demás nodos).
2. En la VM nueva: copiar `certs/ca.pem`, `certs/<vm>/server.*`, `cluster.json` y crear `/opt/dfsha/env`; `docker build -t dfsha .` y `sudo bash deploy/run_datanode.sh n5 10.0.1.25`. (O agregar la VM a `deploy/hosts.txt` con el rol `n5` y repetir `desplegar.sh`.)
3. El nodo se registra solo por heartbeat y recibe bloques nuevos y reparaciones. **No hay rebalanceo** de los bloques ya existentes. Los 3 controladores son fijos (están en `cluster.json`; cambiarlos exige editarlo y reiniciar).

### Limitaciones conocidas
Ver `docs/DECISIONES.md` §4 (incluye: reescritura completa al modificar un archivo, detección de corrupción solo al leer, sin rebalanceo, controladores fijos, metadatos en memoria con un único líder).

## API de control (REST/HTTPS, JSON)
| Operación | Endpoint | Errores relevantes |
|---|---|---|
| registro / login | `POST /v1/register`, `POST /v1/login` | 401 credenciales |
| namespace | `GET /v1/ls`, `GET /v1/stat`, `POST /v1/mkdir`, `POST /v1/rmdir`, `DELETE /v1/files` | 404, 409 NOT_EMPTY/EXISTS |
| iniciar escritura | `POST /v1/uploads` `{path, mode ∈ create\|replace\|append\|modify, expected_version?, block_size?}` | 423 LOCKED, 412 VERSION_CONFLICT, 409 EXISTS, 404 NOT_FOUND (modify), 503 INSUFFICIENT_NODES |
| ubicaciones | `POST /v1/uploads/{id}/allocate`, `/reassign`, `/renew` | 409 LEASE_EXPIRED |
| publicar | `POST /v1/uploads/{id}/commit` `{blocks:[{index,id,len,sha,acks}], content_sha256}` | 422 INSUFFICIENT_REPLICAS, 409 LEASE_EXPIRED, 503 NO_QUORUM |
| abortar | `POST /v1/uploads/{id}/abort` | — |
| lectura | `GET /v1/files/manifest?path=&version=` | 404, 410 VERSION_EXPIRED |
| locks | `POST /v1/locks`, `POST /v1/locks/renew`, `DELETE /v1/locks` | 423 |
| estado (operador) | `GET /v1/cluster` con `X-Admin-Key` | 403 |

Nodos de datos: `PUT/GET /blocks/{id}` (token `data`), `DELETE /blocks/{id}` y `/internal/*` (token `svc`). Errores: 401 sin/alterado/vencido, 403 token fuera de alcance, 422 hash o bloque corrupto.

Ejemplo de manifiesto (`GET /v1/files/manifest`): `{"file_id","version":3,"size":..., "content_sha256", "blocks":[{"index":0,"id":"a1b2-0","offset":0,"length":4194304,"sha256":"…","locations":[{"node":"n1","url":"https://10.0.1.21:9001"}, …]}], "read_token":"…"}`.

## Semántica
- **write()**: crear (`x`, falla si existe), reemplazar completo (`w`, crea si no existe), anexar (`a`) y **modificar un rango** de un archivo existente (`write_at` / CLI `write --offset`). La modificación es *copy-on-write por bloque*: nunca se altera un bloque publicado; solo se reenvían, con identificadores nuevos, los bloques que toca el rango (si es 1 byte, 1 bloque) y la versión nueva comparte el resto con la anterior. No extiende el archivo (para eso, anexar) y reescribe completo cada bloque tocado (no hay deltas). Cada `close()` exitoso crea una versión nueva inmutable; si falla, la anterior sigue vigente.
- **read()**: `open('r')` toma un snapshot de la última versión confirmada; `read(n)` verifica SHA-256 por bloque; al final del archivo devuelve 0 bytes.
- **lock()**: exclusivo por archivo, con lease renovable (30 s) y fencing token creciente. Toda escritura toma el lock automáticamente; un lock explícito sobrevive entre escrituras hasta `unlock`.
- Garantías formales y decisiones: `docs/DECISIONES.md`.

## Estructura
`dfsha/controller.py` (Raft + API + reparación/GC) · `dfsha/datanode.py` · `dfsha/client.py` · `dfsha/cli.py` · `dfsha/common.py` · `scripts/` (certificados, clúster local) · `tests/run_tests.py` · `deploy/` (creación de VMs, despliegue y evidencia en AWS) · `docs/` (decisiones, guía de AWS, prompt) · `results/` · `evidencia/` (se genera en AWS).
