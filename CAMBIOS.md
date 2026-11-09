# Cambios aplicados al proyecto DFSha

| # | Archivo(s) | Cambio | Por qué |
|---|---|---|---|
| 1 | scripts/gen_certs.py | Agrega Authority/Subject Key Identifier a los certificados | Python 3.13 rechazaba los certificados y el clúster nunca arrancaba |
| 2 | scripts/gen_certs.py, README | Un certificado y llave privada por nodo (`--node NOMBRE=IP`); la CA se guarda (ca.key) y se reutiliza para agregar nodos sin tocar los demás | El README copiaba la misma llave privada a todas las VMs; agregar un nodo exigía rehacer todo |
| 3 | dfsha/common.py, deploy/cluster.example.json, docs/DECISIONES.md | `retention` por defecto 360 s (>= `read_ttl` 300 s); advertencia al arrancar si se configura al revés | D8 decía retención >= TTL de lectura pero los valores por defecto la contradecían |
| 4 | dfsha/common.py, controller.py, datanode.py | Los secretos DFSHA_CLUSTER_KEY, DFSHA_ADMIN_KEY y DFSHA_NODE_KEY son obligatorios; sin ellos el proceso no arranca (DFSHA_DEV=1 solo para desarrollo) | Antes caía en valores por defecto públicos |
| 5 | dfsha/controller.py (`owns_block`, `/v1/blocks/corrupt`) | Solo el dueño del archivo puede reportar réplicas corruptas de sus bloques (403 si no) | Cualquier usuario autenticado podía retirar réplicas de bloques ajenos |
| 6 | tests/run_tests.py | Nueva verificación en P6 (bob no puede reportar bloques de ana); `--only` ahora ejecuta P1 como prerrequisito | `--only seguridad` fallaba porque dependía de /big.bin creado por P1 |
| 7 | requirements.txt, .gitignore | Versiones fijadas a las probadas; .gitignore para certs, llaves y env | Despliegue reproducible; evitar subir secretos |
| 8 | README.md, docs/DECISIONES.md | Pasos de certificados y secretos, cómo agregar un nodo, `docker stop` vs `docker kill`, D8/D10 aclarados y limitaciones nuevas | Que lo documentado coincida con lo implementado |

Verificación: suite completa 13/13 en un clúster local (3 controladores + 4 nodos, TLS), Python 3.13. Falta repetirla con Python 3.12 (Dockerfile) y en AWS.
La carpeta results/ conserva los resultados originales del equipo; vuelvan a ejecutar `python tests/run_tests.py` para regenerarla con el código nuevo.

## Revisión posterior (verificada en Python 3.12 con las versiones de requirements.txt)
| # | Archivo | Cambio | Por qué |
|---|---|---|---|
| 9 | dfsha/datanode.py | Valida `DFSHA_CLUSTER_KEY` al arrancar | Sin ella el nodo arrancaba "sano", respondía 500 a cada petición y su hilo de heartbeat moría en silencio |
| 10 | dfsha/controller.py | Valida `DFSHA_ADMIN_KEY` al arrancar | Faltaba solo se descubría al primer `/v1/cluster` (SystemExit dentro de una petición) |

Verificación: `pip install -r requirements.txt` en un entorno limpio con Python 3.12.3 (todas las versiones fijadas existen) y suite completa 13/13.

## Versión 3.1: modificación de un rango por bloque (copy-on-write)
| # | Archivo | Cambio | Por qué |
|---|---|---|---|
| 11 | dfsha/controller.py | Nuevo modo `modify`: `begin_upload` exige archivo existente; `commit_upload` reemplaza solo los bloques enviados (misma posición y tamaño) y comparte el resto con la versión base; valida antes de mutar el estado (`BAD_MODIFY`); el endpoint de commit acepta un subconjunto de posiciones | Responder a la pregunta del profesor sobre cambiar 1 byte: ya no se reescribe todo el archivo |
| 12 | dfsha/client.py | `write_at(path, offset, data)` y modo `'m'` con `modify()`: toma el lock, lee y verifica (SHA-256) solo los bloques afectados, aplica el cambio y los reenvía; error 416 si el rango sale del archivo | Mismo camino de envío con acuses que el resto de escrituras |
| 13 | dfsha/cli.py | `write --offset N` | Poder demostrarlo desde la línea de comandos |
| 14 | tests/run_tests.py | Prueba P8: 1 byte → 1 bloque nuevo, rango que cruza bloques, snapshot anterior intacto, 416/412/423, el GC libera solo los bloques reemplazados | Evidencia verificable |
| 15 | README.md, docs/DECISIONES.md | D4 reescrita (ya no es una simplificación pendiente de aprobación), semántica y limitaciones actualizadas | Que lo documentado coincida con lo implementado |

No cambia el camino de crear, reemplazar ni anexar: las 13 pruebas anteriores se mantienen.

## Preparación del despliegue en AWS
| # | Archivo | Cambio | Por qué |
|---|---|---|---|
| 16 | deploy/run_controller.sh, deploy/run_datanode.sh | `set -a; source /opt/dfsha/env; set +a` | `docker run -e VAR` solo recibe variables exportadas: con un `env` sin `export`, los secretos no llegaban al contenedor y el proceso no arrancaba |
| 17 | docs/GUIA_AWS.md, scripts/aws_bench.py | Guía paso a paso del despliegue en AWS y benchmark contra el clúster desplegado (≥5 repeticiones, mediana/p95, CSV, verificación de hash) | Preparar la evidencia real en AWS que exige el PDF |
| 18 | deploy/desplegar.sh, deploy/hosts.example.txt, .gitignore | Script que automatiza el despliegue por SSH (secretos, certificados por nodo, cluster.json, Docker, arranque y prueba de punta a punta) | Llegar a AWS con el menor trabajo manual posible |
| 19 | deploy/hosts.example.txt, deploy/desplegar.sh | Topología de 4 VMs: cada VM puede llevar varios roles (`c1,n1`); un certificado por VM; un `env` por VM; el cliente corre en la VM con el rol `cli` | Pasar de 8 a 4 VMs sin cambiar el software |
| 20 | deploy/aws_crear_vms.sh | Crea, detiene, inicia y elimina las 4 VMs con la AWS CLI y escribe `hosts.txt` | Menos pasos manuales en la consola |
| 21 | deploy/evidencia_aws.sh, scripts/evidencia_cliente.py | Inyección de fallas (stop/kill/pause), pruebas de concurrencia, modificación, seguridad, cifrado en reposo y bit alterado, con salida guardada en `evidencia/` | Evidencia reproducible con un solo comando |
| 22 | docs/GUIA_AWS.md, docs/PROMPT_CLAUDE_CODE.md | Guía nueva orientada a 4 VMs y prompt para que Claude Code ejecute el despliegue y la evidencia | Reducir el trabajo manual y el tiempo |
| 23 | README.md, docs/DECISIONES.md | Sección de AWS reescrita; limitación de VMs compartidas declarada | Que lo documentado coincida con lo implementado |

Verificación de la versión 3.1: suite completa 14/14 en clúster local (Python 3.13); `aws_bench.py` probado contra un clúster local. Lo que **no** se verificó: Docker, AWS, reinicio automático con `docker kill`.
Verificación de los ítems 18 a 23: sintaxis y modo simulación (`DRY_RUN=1`, `ssh` y `aws` simulados); generación real de secretos, certificados (uno por VM) y cluster.json; los subcomandos de `scripts/evidencia_cliente.py` se ejecutaron contra un clúster local (todas las comprobaciones OK). **No se probó contra VMs reales ni con la AWS CLI real.**
