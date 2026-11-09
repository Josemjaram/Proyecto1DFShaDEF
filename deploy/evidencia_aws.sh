#!/usr/bin/env bash
# Pruebas de evidencia en AWS: inyecta las fallas (docker stop/kill/pause) y guarda la salida en evidencia/*.txt.
# Se ejecuta desde SU computador, después de deploy/desplegar.sh (usa el mismo deploy/hosts.txt y SSH_KEY/SSH_USER).
#
#   bash deploy/evidencia_aws.sh            # todos los pasos
#   bash deploy/evidencia_aws.sh 03 05      # solo esos pasos
#
# Pasos: 00 topología y red · 01 carga y recuperación · 02 caída de un nodo de datos · 03 reinicio automático
#        04 réplica desconectada · 05 failover del líder · 06 pérdida de mayoría · 07 concurrencia
#        08 modificar 1 byte · 09 seguridad · 10 cifrado en reposo y bit alterado · 11 rendimiento · 12 logs
# Cada archivo de evidencia lleva fecha y hora. Los pasos son independientes pero conviene el orden numérico.
set -uo pipefail
cd "$(dirname "$0")/.."

HOSTS=${HOSTS:-deploy/hosts.txt}
SSH_KEY=${SSH_KEY:?Defina SSH_KEY con la ruta de su llave .pem}
SSH_USER=${SSH_USER:-ec2-user}
OUT=evidencia
SSH_OPTS=(-i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 -o ServerAliveInterval=15)
mkdir -p "$OUT/logs"

VMS=(); PRIV=(); PUB=(); ROLES=()
while read -r n priv pub roles _; do
  [[ -z "${n:-}" || "$n" == \#* ]] && continue
  VMS+=("$n"); PRIV+=("$priv"); PUB+=("$pub"); ROLES+=("$roles")
done < "$HOSTS"
idx_of() { for i in "${!VMS[@]}"; do IFS=',' read -ra RS <<< "${ROLES[$i]}"; for r in "${RS[@]}"; do [[ "$r" == "$1" ]] && { echo "$i"; return; }; done; done; }
pub_of() { echo "${PUB[$(idx_of "$1")]}"; }
priv_of() { echo "${PRIV[$(idx_of "$1")]}"; }
role_by_priv() { for i in "${!VMS[@]}"; do [[ "${PRIV[$i]}" == "$1" ]] && { IFS=',' read -ra RS <<< "${ROLES[$i]}"; for r in "${RS[@]}"; do [[ "$r" == c[0-9]* ]] && { echo "$r"; return; }; done; }; done; }
onvm() { local role=$1; shift; ssh "${SSH_OPTS[@]}" "$SSH_USER@$(pub_of "$role")" "$@"; }
cli() { ssh "${SSH_OPTS[@]}" "$SSH_USER@$(pub_of cli)" "source ~/dfsha_cliente_env.sh; cd ~/dfsha; $*"; }
ev() { cli "python3 scripts/evidencia_cliente.py $*"; }
CTRLS=(c1 c2 c3); DATAS=(); for i in "${!VMS[@]}"; do IFS=',' read -ra RS <<< "${ROLES[$i]}"; for r in "${RS[@]}"; do [[ "$r" == n[0-9]* ]] && DATAS+=("$r"); done; done

SELECT=("$@")
want() { [[ ${#SELECT[@]} -eq 0 ]] && return 0; for s in "${SELECT[@]}"; do [[ "$s" == "$1" ]] && return 0; done; return 1; }
RESULT=()
step() { # $1=número $2=nombre ; el cuerpo es la función con ese nombre
  want "$1" || return 0
  local f="$OUT/$1_$2.txt"
  echo; echo "=== $1 $2 -> $f"
  { echo "# $2 — $(date '+%Y-%m-%d %H:%M:%S %Z')"; echo; "paso_$1"; } 2>&1 | tee "$f"
  RESULT+=("$1 $2: ${PIPESTATUS[0]}")
}
sleep_msg() { echo "-- esperando $1 s ($2)"; sleep "$1"; }

paso_00() {
  echo "## Contenedores en cada VM (procesos separados en máquinas distintas)"
  for i in "${!VMS[@]}"; do echo "--- ${VMS[$i]} (${PRIV[$i]})  roles: ${ROLES[$i]}"; ssh "${SSH_OPTS[@]}" "$SSH_USER@${PUB[$i]}" "hostname; sudo docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'"; done
  echo; echo "## Comunicación real por la red privada: desde ${VMS[$(idx_of c1)]} hacia los demás componentes (HTTPS con la CA propia)"
  for r in c2 c3; do echo -n "controlador $r https://$(priv_of $r):8001/health -> "; onvm c1 "curl -s --cacert /opt/dfsha/certs/ca.pem https://$(priv_of $r):8001/health"; echo; done
  for r in "${DATAS[@]}"; do echo -n "nodo de datos $r https://$(priv_of $r):9001/health -> "; onvm c1 "curl -s --cacert /opt/dfsha/certs/ca.pem https://$(priv_of $r):9001/health"; echo; done
  echo; echo "## Estado del clúster"; ev estado
}
paso_01() { ev preparar; ev leer; ev esperar-reparado 90; ev estado; }
paso_02() {
  echo "## Se detiene el nodo de datos n1 (docker stop dfsha-n1)"; onvm n1 "sudo docker stop dfsha-n1"
  echo "## Lectura y escritura con n1 caído"; ev leer; ev escribir
  sleep_msg 25 "n1 pasa a SUSPECT y luego DEAD"; ev estado
  echo "## n1 vuelve (docker start dfsha-n1)"; onvm n1 "sudo docker start dfsha-n1"
  ev esperar-reparado 120; ev estado
}
paso_03() {
  echo "## Caída abrupta de n2 (kill -9 al proceso del contenedor, como un fallo real): Docker debe levantarlo solo (--restart unless-stopped)"
  # 'docker kill' cuenta como parada manual y Docker NO reinicia el contenedor; un kill -9 desde el host sí es un fallo.
  onvm n2 "sudo docker inspect -f 'antes: estado={{.State.Status}} reinicios={{.RestartCount}}' dfsha-n2; sudo kill -9 \$(sudo docker inspect -f '{{.State.Pid}}' dfsha-n2)"; sleep_msg 20 "reinicio automático"
  local R; R=$(onvm n2 "sudo docker inspect -f '{{.State.Status}} {{.RestartCount}}' dfsha-n2")
  onvm n2 "sudo docker inspect -f 'estado={{.State.Status}} reinicios={{.RestartCount}} iniciado={{.State.StartedAt}}' dfsha-n2; sudo docker ps --filter name=dfsha-n2 --format '{{.Names}} {{.Status}}'"
  ev estado; ev leer
  if [[ "$R" == running\ [1-9]* ]]; then echo "  OK    Docker reinició n2 solo (reinicios >= 1)"; else echo "  FALLÓ Docker no reinició n2 ($R)"; return 1; fi
}
paso_04() {
  echo "## Réplica desconectada: se congela n3 (docker pause) durante una escritura"
  onvm n3 "sudo docker pause dfsha-n3"; ev escribir; ev leer
  echo "## n3 se reconecta (docker unpause)"; onvm n3 "sudo docker unpause dfsha-n3"
  ev esperar-reparado 120; ev estado
}
paso_05() {
  sleep_msg 5 "estabilizar"
  L=$(cli "python3 scripts/evidencia_cliente.py estado" | sed -n 's/^líder: https:\/\/\([0-9.]*\):8001.*/\1/p'); LR=$(role_by_priv "$L")
  echo "## El líder actual es $LR ($L). Se detiene su contenedor (docker stop)"; onvm "$LR" "sudo docker stop dfsha-$LR"
  ev escribir; ev estado
  echo "## $LR vuelve como seguidor (docker start)"; onvm "$LR" "sudo docker start dfsha-$LR"; sleep_msg 10 "reingreso"; ev estado
}
paso_06() {
  sleep_msg 5 "estabilizar"
  echo "## Se detienen 2 de los 3 controladores: se pierde la mayoría"; onvm c2 "sudo docker stop dfsha-c2"; onvm c3 "sudo docker stop dfsha-c3"; sleep_msg 8 "sin mayoría"
  echo "## El sistema debe negarse a operar (503), no arriesgar inconsistencia:"
  cli "bash ./dfsha-cli ls / ; echo \"código de salida: \$?\""
  echo "## Se restablece la mayoría"; onvm c2 "sudo docker start dfsha-c2"; onvm c3 "sudo docker start dfsha-c3"
  ev escribir; ev leer
  echo "## Los metadatos y los datos se conservaron:"; ev estado
}
paso_07() { ev concurrencia; }
paso_08() { ev modificar; }
paso_09() { ev seguridad; }
paso_10() {
  echo "## Cifrado en reposo: el texto plano no aparece en el disco de ningún nodo"
  for i in "${!VMS[@]}"; do echo -n "${VMS[$i]}: coincidencias de 'MARCADOR-SECRETO-DFSHA' en /var/lib/dfsha-data = "; ssh "${SSH_OPTS[@]}" "$SSH_USER@${PUB[$i]}" "sudo grep -rl 'MARCADOR-SECRETO-DFSHA' /var/lib/dfsha-data 2>/dev/null | wc -l"; done
  echo "## Primeros bytes de un bloque guardado (cabecera + nonce + texto cifrado):"
  onvm n1 "f=\$(sudo find /var/lib/dfsha-data/blocks -type f | head -1); echo \$f; sudo head -c 36 \$f | od -An -tx1"
  echo; echo "## Bit alterado en el disco de un nodo"
  B=$(cli "python3 scripts/evidencia_cliente.py bloque" | grep '^BLOQUE'); BID=$(echo "$B" | cut -d' ' -f2); BN=$(echo "$B" | cut -d' ' -f3)
  echo "bloque $BID, réplica en el nodo $BN: se altera 1 byte directamente en el disco"
  onvm "$BN" "f=\$(sudo find /var/lib/dfsha-data/blocks -type f -name '$BID' | head -1); echo \$f; sudo python3 -c \"p='\$f'; b=bytearray(open(p,'rb').read()); b[100]^=0xFF; open(p,'wb').write(bytes(b)); print('byte 100 alterado')\""
  echo "## 6 lecturas seguidas: deben devolver siempre el archivo correcto (la copia mala se descarta y se repone)"
  for k in 1 2 3 4 5 6; do echo -n "lectura $k: "; ev leer | grep -E "OK|FALLÓ"; done
  ev esperar-reparado 120; ev estado
}
paso_11() {
  echo "## Rendimiento (64 MiB, 5 repeticiones; bloque de 1/4/8 MiB y 1/2/4 clientes)"
  cli "python3 scripts/aws_bench.py --user demo --password demo-clave-123 --size-mib 64 --reps 5 --out results/aws_perf.csv"
  scp "${SSH_OPTS[@]}" -q "$SSH_USER@$(pub_of cli):dfsha/results/aws_perf.csv" "$OUT/aws_perf.csv" && echo "CSV guardado en $OUT/aws_perf.csv"
}
paso_12() {
  for r in c1 c2 c3 "${DATAS[@]}"; do onvm "$r" "sudo docker logs --tail 200 dfsha-$r 2>&1" > "$OUT/logs/$r.log" 2>&1; echo "log de $r guardado"; done
}

step 00 topologia; step 01 carga_recuperacion; step 02 caida_nodo_datos; step 03 reinicio_automatico
step 04 replica_desconectada; step 05 failover_lider; step 06 perdida_mayoria; step 07 concurrencia
step 08 modificar_un_byte; step 09 seguridad; step 10 reposo_y_bit_alterado; step 11 rendimiento; step 12 logs

echo; echo "== Resumen (código de salida de cada paso; 0 = sin fallos)"; printf '%s\n' "${RESULT[@]}" | tee "$OUT/resumen.txt"
echo "Evidencia en $OUT/. Revise los archivos, tome capturas de la consola de AWS y de las terminales para el informe y el video."
