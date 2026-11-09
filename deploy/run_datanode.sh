#!/usr/bin/env bash
# Uso en cada VM de datos:  ./deploy/run_datanode.sh n1 10.0.1.21
set -euo pipefail
ID=$1; IP=$2
set -a; source /opt/dfsha/env; set +a   # (set -a exporta las variables para que `docker run -e VAR` las reciba)
# DFSHA_CLUSTER_KEY, DFSHA_NODE_KEY (clave maestra de cifrado en reposo)
docker run -d --restart unless-stopped --name dfsha-$ID --network host \
  -v /opt/dfsha:/opt/dfsha -v /var/lib/dfsha-data:/data \
  -e DFSHA_CLUSTER_KEY -e DFSHA_NODE_KEY -e DFSHA_CA=/opt/dfsha/certs/ca.pem \
  -e DFSHA_TLS_CERT=/opt/dfsha/certs/server.pem -e DFSHA_TLS_KEY=/opt/dfsha/certs/server.key \
  dfsha dfsha.datanode --config /opt/dfsha/cluster.json --id "$ID" \
  --listen 0.0.0.0:9001 --advertise "https://$IP:9001" --data /data
