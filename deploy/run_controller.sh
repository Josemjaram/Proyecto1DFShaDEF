#!/usr/bin/env bash
# Uso en cada VM de control:  ./deploy/run_controller.sh c1 10.0.1.11   (id e IP privada de ESTA máquina)
# Requiere: /opt/dfsha/cluster.json, /opt/dfsha/certs/{ca.pem,server.pem,server.key} y variables en /opt/dfsha/env
set -euo pipefail
ID=$1; IP=$2
set -a; source /opt/dfsha/env; set +a   # (set -a exporta las variables para que `docker run -e VAR` las reciba)
# DFSHA_CLUSTER_KEY, DFSHA_ADMIN_KEY (secretos: NO van al repositorio)
docker run -d --restart unless-stopped --name dfsha-$ID --network host \
  -v /opt/dfsha:/opt/dfsha -v /var/lib/dfsha-state:/state \
  -e DFSHA_CLUSTER_KEY -e DFSHA_ADMIN_KEY -e DFSHA_STATE_DIR=/state -e DFSHA_CA=/opt/dfsha/certs/ca.pem \
  -e DFSHA_TLS_CERT=/opt/dfsha/certs/server.pem -e DFSHA_TLS_KEY=/opt/dfsha/certs/server.key \
  dfsha dfsha.controller --config /opt/dfsha/cluster.json --id "$ID" --listen 0.0.0.0:8001
