#!/usr/bin/env bash
# Despliegue automático de DFSha en las VMs de AWS Academy, desde SU computador (Linux, macOS, WSL o Git Bash).
# Topología por defecto (4 VMs): vm1-vm3 = controlador + nodo de datos; vm4 = nodo de datos + cliente.
#
# Qué hace (todo por SSH):
#   1. Genera secretos (una vez), la CA, UN certificado por VM y el cluster.json a partir de hosts.txt.
#   2. En cada VM: instala Docker, copia código, certificados, cluster.json y env, y construye la imagen.
#   3. Arranca los 3 controladores y luego los nodos de datos (deploy/run_controller.sh y run_datanode.sh).
#   4. Prepara el cliente y hace una prueba de punta a punta (put, get, comparación de hash, estado del clúster).
#
# Uso:
#   bash deploy/aws_crear_vms.sh           # crea las 4 VMs y escribe deploy/hosts.txt (o escriba hosts.txt a mano)
#   export SSH_KEY=~/Downloads/labsuser.pem SSH_USER=ec2-user
#   bash deploy/desplegar.sh               # DRY_RUN=1 bash deploy/desplegar.sh solo muestra lo que haría
#
# Se puede repetir sin problema: recrea los contenedores y conserva secretos y certificados.
# ca.key y secrets.generated.env se quedan en su computador (el .gitignore los excluye): NO los suba a GitHub.
set -euo pipefail
cd "$(dirname "$0")/.."

HOSTS=${HOSTS:-deploy/hosts.txt}
SSH_KEY=${SSH_KEY:?Defina SSH_KEY con la ruta de su llave .pem}
SSH_USER=${SSH_USER:-ec2-user}
DRY_RUN=${DRY_RUN:-0}
SECRETS=deploy/secrets.generated.env
LOGS=deploy/logs
SSH_OPTS=(-i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 -o ServerAliveInterval=15)

[[ -f "$HOSTS" ]] || { echo "Falta $HOSTS (ejecute deploy/aws_crear_vms.sh o copie deploy/hosts.example.txt y edítelo)"; exit 1; }
mkdir -p "$LOGS" certs

# ---------------------------------------------------------------- lectura de hosts
VMS=(); PRIV=(); PUB=(); ROLES=()
while read -r n priv pub roles _; do
  [[ -z "${n:-}" || "$n" == \#* ]] && continue
  [[ -n "${roles:-}" ]] || { echo "Línea sin roles para $n en $HOSTS"; exit 1; }
  VMS+=("$n"); PRIV+=("$priv"); PUB+=("$pub"); ROLES+=("$roles")
done < "$HOSTS"
CTRL_VM=(); CTRL_ID=(); DATA_VM=(); DATA_ID=(); CLI_VM=""
for i in "${!VMS[@]}"; do
  nc=0; nd=0
  IFS=',' read -ra RS <<< "${ROLES[$i]}"
  for r in "${RS[@]}"; do
    case "$r" in
      cli) CLI_VM="$i";;
      c[0-9]*) CTRL_VM+=("$i"); CTRL_ID+=("$r"); nc=$((nc+1));;
      n[0-9]*) DATA_VM+=("$i"); DATA_ID+=("$r"); nd=$((nd+1));;
      *) echo "Rol desconocido '$r' en ${VMS[$i]}"; exit 1;;
    esac
  done
  [[ $nc -le 1 && $nd -le 1 ]] || { echo "${VMS[$i]}: como máximo un controlador y un nodo de datos por VM"; exit 1; }
done
[[ ${#CTRL_ID[@]} -eq 3 ]] || { echo "Se necesitan exactamente 3 controladores; hay ${#CTRL_ID[@]}"; exit 1; }
[[ ${#DATA_ID[@]} -ge 4 ]] || { echo "Se necesitan al menos 4 nodos de datos; hay ${#DATA_ID[@]}"; exit 1; }
[[ -n "$CLI_VM" ]] || { echo "Falta una VM con el rol cli"; exit 1; }
echo "VMs: ${#VMS[@]}  Controladores: ${#CTRL_ID[@]}  Nodos de datos: ${#DATA_ID[@]}  Cliente en: ${VMS[$CLI_VM]}"

say() { echo; echo "== $*"; }
ssh_run() { local ip=$1; shift; if [[ $DRY_RUN == 1 ]]; then echo "[dry-run] ssh $SSH_USER@$ip: $*"; else ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" "$@"; fi; }
ssh_put() { local ip=$1 dest=$2; if [[ $DRY_RUN == 1 ]]; then echo "[dry-run] escribir $dest en $ip"; cat >/dev/null; else ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" "umask 077; cat > $dest"; fi; }
role_of() { # $1 = índice de VM, $2 = prefijo (c|n) -> id del rol o vacío
  IFS=',' read -ra RS <<< "${ROLES[$1]}"; for r in "${RS[@]}"; do [[ "$r" == $2[0-9]* ]] && { echo "$r"; return; }; done; echo ""; }

# ---------------------------------------------------------------- 1. secretos, certificados, cluster.json
say "1. Secretos, certificados y cluster.json"
if [[ ! -f "$SECRETS" ]]; then
  ( umask 077
    echo "DFSHA_CLUSTER_KEY=$(openssl rand -hex 32)" >  "$SECRETS"
    echo "DFSHA_ADMIN_KEY=$(openssl rand -hex 32)"   >> "$SECRETS" )
  echo "Secretos generados en $SECRETS (guárdelos en un lugar privado)"
fi
for d in "${DATA_ID[@]}"; do grep -q "^NODE_KEY_${d}=" "$SECRETS" || echo "NODE_KEY_${d}=$(openssl rand -hex 32)" >> "$SECRETS"; done
get_secret() { grep "^$1=" "$SECRETS" | head -1 | cut -d= -f2-; }

NODEARGS=()
for i in "${!VMS[@]}"; do [[ -f "certs/${VMS[$i]}/server.pem" ]] || NODEARGS+=(--node "${VMS[$i]}=${PRIV[$i]}"); done
if [[ ${#NODEARGS[@]} -gt 0 ]]; then
  python3 scripts/gen_certs.py certs "${NODEARGS[@]}"   # un certificado por VM (lo usan el controlador y el nodo de datos de esa VM)
else
  echo "Certificados ya generados: se reutilizan"
fi

CTRL_SPEC=""; for k in "${!CTRL_ID[@]}"; do CTRL_SPEC+="${CTRL_ID[$k]}=${PRIV[${CTRL_VM[$k]}]} "; done
python3 - $CTRL_SPEC > deploy/cluster.generated.json <<'PY'
import json, sys
ctrl = []
for a in sys.argv[1:]:
    cid, ip = a.split("=")
    ctrl.append({"id": cid, "raft": f"{ip}:4321", "api": f"https://{ip}:8001"})
ctrl.sort(key=lambda c: c["id"])
base = json.load(open("deploy/cluster.example.json"))
base["controllers"] = ctrl
print(json.dumps(base, indent=2))
PY
echo "cluster.generated.json listo (3 controladores)"

# ---------------------------------------------------------------- 2. preparar cada VM
prepare() {
  local i=$1 n=${VMS[$1]} ip=${PUB[$1]} c d
  c=$(role_of "$i" c); d=$(role_of "$i" n)
  echo "[$n] instalando Docker y copiando archivos"
  ssh_run "$ip" 'command -v docker >/dev/null || { if command -v dnf >/dev/null; then sudo dnf install -y docker; else sudo apt-get update -y && sudo apt-get install -y docker.io; fi; }; sudo systemctl enable --now docker; sudo mkdir -p /opt/dfsha/certs /var/lib/dfsha-state /var/lib/dfsha-data; sudo chown -R "$USER" /opt/dfsha; mkdir -p ~/dfsha'
  if [[ $DRY_RUN == 1 ]]; then echo "[dry-run] copiar código (tar) y certificados de $n a $ip"; else
    tar czf - --exclude=certs --exclude=__pycache__ --exclude=.git --exclude='deploy/logs' --exclude='*.generated.*' --exclude='deploy/hosts.txt' --exclude=evidencia . \
      | ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" "tar xzf - -C ~/dfsha"
    scp "${SSH_OPTS[@]}" -q certs/ca.pem "certs/$n/server.pem" "certs/$n/server.key" "$SSH_USER@$ip:/opt/dfsha/certs/"
    scp "${SSH_OPTS[@]}" -q deploy/cluster.generated.json "$SSH_USER@$ip:/opt/dfsha/cluster.json"
  fi
  { echo "DFSHA_CLUSTER_KEY=$(get_secret DFSHA_CLUSTER_KEY)"
    [[ -n "$c" ]] && echo "DFSHA_ADMIN_KEY=$(get_secret DFSHA_ADMIN_KEY)"
    [[ -n "$d" ]] && echo "DFSHA_NODE_KEY=$(get_secret NODE_KEY_$d)"
    true; } | ssh_put "$ip" /opt/dfsha/env
  ssh_run "$ip" 'chmod 600 /opt/dfsha/certs/server.key /opt/dfsha/env; cd ~/dfsha && sudo docker build -t dfsha . >/tmp/dfsha-build.log 2>&1 || { tail -20 /tmp/dfsha-build.log; exit 1; }'
  echo "[$n] imagen construida"
}
say "2. Preparando las VMs (en paralelo; registro en $LOGS/<nombre>.log)"
PIDS=()
for i in "${!VMS[@]}"; do prepare "$i" > "$LOGS/${VMS[$i]}.log" 2>&1 & PIDS+=($!); done
FAIL=0
for i in "${!PIDS[@]}"; do
  if wait "${PIDS[$i]}"; then echo "  ${VMS[$i]}: listo"; else echo "  ${VMS[$i]}: FALLÓ (vea $LOGS/${VMS[$i]}.log)"; FAIL=1; fi
done
[[ $FAIL == 0 ]] || { echo "Corrija lo anterior y vuelva a ejecutar el script."; exit 1; }

# ---------------------------------------------------------------- 3. arrancar
say "3. Arrancando controladores y nodos de datos"
for k in "${!CTRL_ID[@]}"; do
  i=${CTRL_VM[$k]}; id=${CTRL_ID[$k]}
  ssh_run "${PUB[$i]}" "sudo docker rm -f dfsha-$id >/dev/null 2>&1 || true; cd ~/dfsha && sudo bash ./deploy/run_controller.sh $id ${PRIV[$i]}"
done
sleep 5
for k in "${!DATA_ID[@]}"; do
  i=${DATA_VM[$k]}; id=${DATA_ID[$k]}
  ssh_run "${PUB[$i]}" "sudo docker rm -f dfsha-$id >/dev/null 2>&1 || true; cd ~/dfsha && sudo bash ./deploy/run_datanode.sh $id ${PRIV[$i]}"
done

# ---------------------------------------------------------------- 4. cliente y prueba de punta a punta
say "4. Cliente y prueba de punta a punta (${VMS[$CLI_VM]})"
ip=${PUB[$CLI_VM]}
# Las dependencias fijadas exigen Python >= 3.10 y Amazon Linux 2023 trae 3.9: se instala 3.12 y ~/bin/python3 apunta a él.
ssh_run "$ip" 'if command -v dnf >/dev/null; then sudo dnf install -y python3.12 python3.12-pip; PY=python3.12; else sudo apt-get update -y && sudo apt-get install -y python3-pip; PY=python3; fi; mkdir -p ~/bin && ln -sf "$(command -v $PY)" ~/bin/python3 && ln -sf "$(command -v $PY)" ~/bin/python'
URLS=""; for k in "${!CTRL_ID[@]}"; do URLS+="https://${PRIV[${CTRL_VM[$k]}]}:8001,"; done; URLS=${URLS%,}
printf 'export DFSHA_CONTROLLERS=%s\nexport DFSHA_CA=/opt/dfsha/certs/ca.pem\nexport DFSHA_ADMIN_KEY=%s\nexport PYTHONPATH=$HOME/dfsha\nexport PATH=$HOME/bin:$HOME/.local/bin:$PATH\n' "$URLS" "$(get_secret DFSHA_ADMIN_KEY)" | ssh_put "$ip" '~/dfsha_cliente_env.sh'
ssh_run "$ip" 'cd ~/dfsha && export PATH=$HOME/bin:$PATH && (python3 -m pip install --user -q -r requirements.txt || python3 -m pip install --user -q --break-system-packages -r requirements.txt)'
if [[ $DRY_RUN == 1 ]]; then echo "[dry-run] prueba put/get/hash/cluster desde el cliente"; else
  ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" "NDATA=${#DATA_ID[@]} bash -s" <<'REMOTE'
set -e
source ~/dfsha_cliente_env.sh; cd ~/dfsha
echo "esperando a que el clúster elija líder y los $NDATA nodos de datos se registren..."
ok=0
for t in $(seq 1 30); do
  if out=$(bash ./dfsha-cli cluster 2>/dev/null) && echo "$out" | grep -q '"leader_api": "'; then
    n=$(echo "$out" | grep -c '"state": "ALIVE"' || true); [ "${n:-0}" -ge "$NDATA" ] && { ok=1; break; }
  fi
  sleep 5
done
[ $ok = 1 ] || { echo "El clúster no quedó listo en 2,5 minutos. Revise: sudo docker logs dfsha-<id> en cada VM"; bash ./dfsha-cli cluster || true; exit 1; }
bash ./dfsha-cli register --user demo --password demo-clave-123 2>/dev/null || bash ./dfsha-cli login --user demo --password demo-clave-123
head -c 32M /dev/urandom > /tmp/prueba.bin
bash ./dfsha-cli put /tmp/prueba.bin /prueba.bin
bash ./dfsha-cli get /prueba.bin /tmp/prueba.out
A=$(sha256sum /tmp/prueba.bin | cut -d' ' -f1); B=$(sha256sum /tmp/prueba.out | cut -d' ' -f1)
echo "hash original: $A"; echo "hash recuperado: $B"
[ "$A" = "$B" ] && echo "PRUEBA DE PUNTA A PUNTA: OK" || { echo "PRUEBA DE PUNTA A PUNTA: FALLÓ"; exit 1; }
bash ./dfsha-cli cluster
REMOTE
fi

say "Listo"
echo "Siguiente paso: bash deploy/evidencia_aws.sh (pruebas de fallos y evidencia) y python3 scripts/aws_bench.py desde el cliente."
echo "Guarde $SECRETS y certs/ca.key fuera del repositorio."
