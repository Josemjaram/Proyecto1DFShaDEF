# Guía de despliegue en AWS Academy: 4 VMs, casi todo automático

Objetivo: dejar DFSha corriendo en **4 máquinas virtuales** del Learner Lab, demostrar las fallas que pide el enunciado y guardar la evidencia, haciendo a mano lo mínimo. El enunciado pide desplegar en máquinas virtuales de AWS Academy, con procesos o contenedores separados, y demostrar comunicación real entre nodos por red; no exige un número de VMs.

> **Estado de verificación.** El código se probó en un clúster local (suite sin fallos, ver `results/report.md`) y esta guía **se ejecutó completa en AWS Academy (us-east-1, 4 VMs t3.small) el 8 de noviembre de 2026**: despliegue, prueba de punta a punta y pasos de evidencia 00 a 12 sin ningún `FALLÓ`. La salida está en `evidencia/`. Si un paso falla, el mensaje dice qué falló; la sección "Si algo falla" lista lo más probable. Durante esa ejecución se corrigieron dos cosas: el cliente de vm4 usa Python 3.12 (Amazon Linux 2023 trae 3.9 y las dependencias exigen 3.10 o más) y el paso 03 simula la caída con `kill -9` (ver más abajo).

## 1. Qué se monta

| VM | Contenedores Docker | Rol |
|---|---|---|
| vm1 | `dfsha-c1` + `dfsha-n1` | controlador Raft 1 + nodo de datos 1 |
| vm2 | `dfsha-c2` + `dfsha-n2` | controlador Raft 2 + nodo de datos 2 |
| vm3 | `dfsha-c3` + `dfsha-n3` | controlador Raft 3 + nodo de datos 3 |
| vm4 | `dfsha-n4` + el cliente | nodo de datos 4 + CLI y pruebas |

- Sigue habiendo 3 controladores (Raft con mayoría 2 de 3) y 4 nodos de datos, en **procesos separados dentro de contenedores separados**, y toda la comunicación entre VMs va por la red privada con HTTPS/TLS 1.3.
- Los 3 controladores están en VMs distintas: se tolera la caída de una VM completa. Cuando cae una VM caen a la vez un controlador y un nodo de datos; quedan 2 controladores y 3 nodos, suficientes para seguir (la escritura confirma con 2 copias y la tercera se repone en los 3 nodos vivos).
- **Limitación a declarar en el informe**: por el presupuesto y límites del Learner Lab se comparten VMs entre controlador y nodo de datos. No cambia el software; solo reduce el aislamiento de fallas.

## 2. Una sola vez, en su computador

1. Instalar: Git, Python 3.10 o superior, OpenSSL, `ssh`/`scp` y la [AWS CLI](https://aws.amazon.com/cli/). En Windows usen **WSL (Ubuntu)** y trabajen dentro de WSL: los scripts son de bash.
2. Clonar el repositorio y, dentro de la carpeta: `pip install -r requirements.txt`.
3. En el Learner Lab, botón **Start Lab**. Cuando el círculo esté en verde, abran **AWS Details**:
   - **AWS CLI: Show** → copien el bloque a `~/.aws/credentials`. Cambia cada vez que reinicien el laboratorio y dura unas 4 horas.
   - **Download PEM** → `labsuser.pem`; luego `chmod 400 labsuser.pem`.
4. Exporten (cada vez que abran una terminal):
   ```bash
   export SSH_KEY=~/labsuser.pem
   export SSH_USER=ec2-user
   export AWS_REGION=us-east-1        # la región del laboratorio (arriba a la izquierda en la consola)
   ```

## 3. Los cinco comandos

```bash
bash deploy/aws_crear_vms.sh        # 1. crea las 4 VMs, el grupo de seguridad y escribe deploy/hosts.txt   (~3 min)
bash deploy/desplegar.sh            # 2. instala Docker, copia todo, construye, arranca y prueba de punta a punta (~10 min)
bash deploy/evidencia_aws.sh        # 3. inyecta las fallas, guarda la salida en evidencia/ y mide rendimiento   (~25 min)
bash deploy/aws_crear_vms.sh detener   # 4. al terminar la sesión: detiene las VMs para ahorrar presupuesto
bash deploy/aws_crear_vms.sh iniciar   # 5. al volver: las inicia y actualiza hosts.txt (las IP públicas cambian; las privadas no)
```

Detalle de cada uno:

1. **`aws_crear_vms.sh`** crea 4 instancias `t3.small` con Amazon Linux 2023 y 20 GB, con la llave `vockey` del laboratorio. El grupo de seguridad `dfsha-sg` abre SSH solo desde su IP y los puertos 8001 (API de controladores), 4321 (Raft) y 9001 (nodos de datos) solo entre las VMs. Escribe `deploy/hosts.txt`.
2. **`desplegar.sh`** genera los secretos (`deploy/secrets.generated.env`), la CA y un certificado por VM, y el `cluster.json`. En cada VM instala Docker, copia el código, construye la imagen y arranca los contenedores (controladores primero). Termina con una prueba de punta a punta: sube 32 MiB, los baja y compara el hash. Se puede repetir sin problema. Antes de la primera vez pueden ver lo que haría con `DRY_RUN=1 bash deploy/desplegar.sh`.
3. **`evidencia_aws.sh`** ejecuta los pasos 00 a 12 (ver la tabla de abajo). Para repetir solo algunos: `bash deploy/evidencia_aws.sh 03 05`.

## 4. Qué demuestra cada paso de la evidencia

| Paso | Qué hace | Prueba del enunciado | Resultado esperado |
|---|---|---|---|
| 00 | `docker ps` en cada VM y `curl` HTTPS entre VMs | comunicación real por red | contenedores en 4 VMs; `/health` responde entre VMs |
| 01 | sube 64 MiB (16 bloques de 4 MiB) y los recupera | carga y recuperación | bloques en 4 nodos; hash idéntico; 3 copias |
| 02 | `docker stop` de n1; lee y escribe; lo vuelve a iniciar | replicación y fallos | lecturas y escrituras siguen; n1 pasa a DEAD; se repara a 3 copias |
| 03 | `kill -9` al proceso de n2 (caída real; `docker kill` cuenta como parada manual y Docker no lo reinicia) | disponibilidad / reinicio automático | Docker lo levanta solo (`RestartCount` ≥ 1); si no, el paso marca `FALLÓ` |
| 04 | `docker pause` de n3 durante una escritura | consistencia | la escritura confirma con ≥ 2 copias; al volver, n3 se pone al día |
| 05 | detiene al líder de Raft | alta disponibilidad | nuevo líder y escritura aceptada en pocos segundos |
| 06 | detiene 2 de los 3 controladores | consistencia fuerte (CP) | respuesta 503: no opera sin mayoría; al volver, nada se perdió |
| 07 | dos clientes escriben el mismo archivo | concurrencia | el segundo recibe 423 LOCKED; sin mezcla de bytes |
| 08 | cambia 1 byte de un archivo de 4 bloques | acceso RF3 | solo se reenvía 1 bloque; los otros 3 se comparten |
| 09 | accesos con y sin token, otro usuario, TLS | seguridad | 401/403 donde corresponde; TLS 1.3; CA desconocida rechazada |
| 10 | busca texto plano en los discos y altera 1 byte de un bloque | seguridad (reposo e integridad) | 0 coincidencias de texto plano; lecturas correctas y copia mala repuesta |
| 11 | `aws_bench.py`, 5 repeticiones | rendimiento (≥ 2 condiciones) | `evidencia/aws_perf.csv` con mediana y p95 |
| 12 | guarda los logs de cada contenedor | trazabilidad | `evidencia/logs/` |

Cada archivo de `evidencia/` lleva fecha y hora. Al terminar, `evidencia/resumen.txt` lista el código de salida del último comando de cada paso. Revisen los archivos a mano: `FALLÓ` en alguna línea significa que esa comprobación no se cumplió.

## 5. Qué guardar para el informe y el video

- La carpeta `evidencia/` completa (se puede subir al repositorio: no contiene secretos).
- Capturas de la consola de AWS: las 4 instancias, el grupo de seguridad y una terminal con `deploy/hosts.txt`.
- Para el video: ejecuten los pasos 02, 05 y 06 en vivo (o muestren la salida ya grabada) con una terminal con `docker logs -f dfsha-c1` al lado.
- En el informe: tipo de instancia, región, la limitación de las VMs compartidas y que las cifras son de AWS (las de `results/` son de localhost y no valen como mediciones de AWS).

## 6. Si algo falla

| Síntoma | Causa probable | Qué hacer |
|---|---|---|
| `Credenciales de AWS inválidas o vencidas` | el laboratorio se reinició o pasaron 4 h | copiar de nuevo el bloque «AWS CLI» a `~/.aws/credentials` |
| `InvalidKeyPair.NotFound` | la llave no se llama `vockey` | `KEY_NAME=<nombre> bash deploy/aws_crear_vms.sh` |
| `VcpuLimitExceeded` / `InsufficientInstanceCapacity` | límite del laboratorio o falta de capacidad | `INSTANCE_TYPE=t3.micro bash …` (puede ser lento) o esperar y repetir |
| `Permission denied (publickey)` | `SSH_KEY` o `SSH_USER` incorrectos | revisar la ruta; en Ubuntu use `SSH_USER=ubuntu` |
| `Connection timed out` en SSH | su IP cambió y el SG ya no la permite | agregar su IP actual al puerto 22 del grupo `dfsha-sg` en la consola |
| `desplegar.sh` dice «FALLÓ» en una VM | Docker no instalado o error de construcción | leer `deploy/logs/<vm>.log` |
| El clúster no queda listo | falta un puerto en el SG o un secreto | `sudo docker logs dfsha-c1` en vm1; revisar puertos 8001/4321/9001 |
| `certificate verify failed` | la IP privada de una VM cambió | `rm -r certs deploy/secrets.generated.env` y repetir `desplegar.sh` |
| `UNAVAILABLE` permanente | menos de 2 controladores vivos | `sudo docker start dfsha-cX` en la VM correspondiente |

Cuando el laboratorio se detiene, las IP privadas se conservan pero las públicas cambian: `bash deploy/aws_crear_vms.sh iniciar` actualiza `hosts.txt`. Los contenedores vuelven solos porque Docker está habilitado y la política es `--restart unless-stopped`; el primer arranque tarda un par de minutos.

## 7. Plan B: todo a mano (si falla la AWS CLI)

1. En la consola: **EC2 → Lanzar instancias**, 4 veces: Amazon Linux 2023, `t3.small`, 20 GB, llave `vockey`, mismo grupo de seguridad (SSH desde su IP; TCP 8001, 4321 y 9001 con origen el propio grupo). Pónganles el nombre `dfsha-vm1` … `dfsha-vm4`.
2. Copien `deploy/hosts.example.txt` a `deploy/hosts.txt` y pongan las IP privadas y públicas de cada VM (los roles se dejan igual).
3. Sigan desde el comando 2 (`desplegar.sh`).

## 8. Despliegue manual (sin `desplegar.sh`), por si hace falta entender o depurar

En cada VM: `sudo mkdir -p /opt/dfsha/certs /var/lib/dfsha-state /var/lib/dfsha-data`, copiar a `/opt/dfsha/` el `cluster.json` y los `certs/ca.pem` y `certs/<vm>/server.pem|key`, crear `/opt/dfsha/env` con `DFSHA_CLUSTER_KEY` (igual en todas), `DFSHA_ADMIN_KEY` (controladores) y `DFSHA_NODE_KEY` (nodo de datos), y ejecutar `sudo docker build -t dfsha .`, `sudo bash deploy/run_controller.sh c1 <ip privada>` y `sudo bash deploy/run_datanode.sh n1 <ip privada>`. `desplegar.sh` hace exactamente esto por SSH.
