#!/usr/bin/env bash
# Crea (o administra) las 4 VMs de DFSha en AWS Academy Learner Lab con la AWS CLI y escribe deploy/hosts.txt.
#
# Requisitos (una sola vez):
#   1. AWS CLI instalada (https://aws.amazon.com/cli/) y Python 3.
#   2. En el Learner Lab: "AWS Details" -> "AWS CLI: Show" -> copie el bloque a ~/.aws/credentials
#      (cambia cada vez que reinicia el laboratorio y dura unas 4 horas).
#   3. En el mismo panel: "Download PEM" (labsuser.pem) -> chmod 400 labsuser.pem
#
# Uso:
#   bash deploy/aws_crear_vms.sh            # crea las 4 VMs (si ya existen, las reutiliza) y escribe hosts.txt
#   bash deploy/aws_crear_vms.sh ips        # vuelve a escribir hosts.txt (las IP públicas cambian al reiniciar el lab)
#   bash deploy/aws_crear_vms.sh detener    # detiene las VMs (ahorra presupuesto; las IP privadas se conservan)
#   bash deploy/aws_crear_vms.sh iniciar    # las vuelve a iniciar y actualiza hosts.txt
#   bash deploy/aws_crear_vms.sh estado     # muestra las VMs
#   bash deploy/aws_crear_vms.sh eliminar   # las elimina (hay que volver a desplegar si se crean de nuevo)
#
# Variables opcionales: AWS_REGION (us-east-1), KEY_NAME (vockey), INSTANCE_TYPE (t3.small), DISK_GB (20)
# NOTA: este script no se pudo probar fuera de AWS; si un comando falla, el mensaje de la AWS CLI dice la causa
# (permisos del laboratorio, nombre de la llave, límites). Crear las 4 VMs a mano en la consola es equivalente.
set -euo pipefail
cd "$(dirname "$0")/.."

export AWS_DEFAULT_REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}
KEY_NAME=${KEY_NAME:-vockey}
INSTANCE_TYPE=${INSTANCE_TYPE:-t3.small}
DISK_GB=${DISK_GB:-20}
SG_NAME=dfsha-sg
TAG=dfsha
ROLES=("c1,n1" "c2,n2" "c3,n3" "n4,cli")   # vm1..vm4
ACCION=${1:-crear}

command -v aws >/dev/null || { echo "Falta la AWS CLI"; exit 1; }
aws sts get-caller-identity >/dev/null 2>&1 || { echo "Credenciales de AWS inválidas o vencidas: copie de nuevo el bloque 'AWS CLI' del Learner Lab a ~/.aws/credentials"; exit 1; }

ids_vivas() {  # ids de las VMs del proyecto, ordenados por nombre (dfsha-vm1..4)
  aws ec2 describe-instances --filters "Name=tag:Project,Values=$TAG" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,InstanceId]' --output text | sort | awk '{print $2}' | tr '\n' ' '
}

escribir_hosts() {
  local n=0 i priv pub
  local ids; ids=$(ids_vivas)
  [[ -n "${ids// /}" ]] || { echo "No hay VMs con la etiqueta Project=$TAG"; return 1; }
  { echo "# nombre  ip_privada  ip_publica  roles   (generado por deploy/aws_crear_vms.sh)"
    for i in $ids; do
      read -r priv pub <<< "$(aws ec2 describe-instances --instance-ids "$i" --query 'Reservations[0].Instances[0].[PrivateIpAddress,PublicIpAddress]' --output text)"
      echo "vm$((n+1))  $priv  $pub  ${ROLES[$n]}"; n=$((n+1))
    done; } > deploy/hosts.txt
  echo "deploy/hosts.txt escrito:"; cat deploy/hosts.txt
}

case "$ACCION" in
  estado) aws ec2 describe-instances --filters "Name=tag:Project,Values=$TAG" --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,InstanceId,State.Name,PrivateIpAddress,PublicIpAddress]' --output table; exit 0;;
  ips) escribir_hosts; exit $?;;
  detener) ids=$(ids_vivas); [[ -n "$ids" ]] && aws ec2 stop-instances --instance-ids $ids --output text >/dev/null && echo "VMs detenidas"; exit 0;;
  iniciar) ids=$(ids_vivas); aws ec2 start-instances --instance-ids $ids --output text >/dev/null; aws ec2 wait instance-running --instance-ids $ids; sleep 20; escribir_hosts; exit $?;;
  eliminar) ids=$(ids_vivas); [[ -n "$ids" ]] && aws ec2 terminate-instances --instance-ids $ids --output text >/dev/null && echo "VMs eliminadas"; exit 0;;
  crear) ;;
  *) echo "Acción desconocida: $ACCION"; exit 1;;
esac

# ---------------------------------------------------------------- crear
existentes=$(ids_vivas | wc -w)
if [[ $existentes -ge 4 ]]; then echo "Ya existen $existentes VMs; no se crean otras."; escribir_hosts; exit $?; fi
[[ $existentes -eq 0 ]] || { echo "Hay $existentes VMs de un intento anterior: ejecute 'eliminar' y vuelva a crear."; exit 1; }

VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
[[ "$VPC" != "None" ]] || { echo "No hay VPC por defecto; cree las VMs a mano"; exit 1; }
SUBNET=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC" "Name=default-for-az,Values=true" --query 'Subnets[0].SubnetId' --output text)
AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query 'Parameter.Value' --output text 2>/dev/null || true)
if [[ -z "$AMI" || "$AMI" == "None" ]]; then
  AMI=$(aws ec2 describe-images --owners amazon --filters 'Name=name,Values=al2023-ami-2023.*-x86_64' 'Name=state,Values=available' --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text)
fi
echo "VPC $VPC  subred $SUBNET  AMI $AMI (Amazon Linux 2023, usuario SSH: ec2-user)"

SG=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$SG_NAME" "Name=vpc-id,Values=$VPC" --query 'SecurityGroups[0].GroupId' --output text)
if [[ "$SG" == "None" ]]; then
  SG=$(aws ec2 create-security-group --group-name "$SG_NAME" --description "DFSha: SSH desde mi IP; 8001, 4321 y 9001 solo entre las VMs" --vpc-id "$VPC" --query GroupId --output text)
  MYIP=$(curl -s https://checkip.amazonaws.com | tr -d '\n' || true)
  aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr "${MYIP:-0.0.0.0}/32" >/dev/null 2>&1 || \
    aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr 0.0.0.0/0 >/dev/null
  for p in 8001 4321 9001; do
    aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port "$p" --source-group "$SG" >/dev/null
  done
  echo "Security group $SG creado: SSH desde ${MYIP:-cualquier IP}; 8001, 4321 y 9001 solo entre las VMs del grupo"
fi

IDS=()
for n in 1 2 3 4; do
  id=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$INSTANCE_TYPE" --key-name "$KEY_NAME" \
        --security-group-ids "$SG" --subnet-id "$SUBNET" --associate-public-ip-address \
        --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=$DISK_GB,VolumeType=gp3}" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=dfsha-vm$n},{Key=Project,Value=$TAG}]" \
        --query 'Instances[0].InstanceId' --output text)
  IDS+=("$id"); echo "creada dfsha-vm$n ($id)"
done
echo "esperando a que las VMs estén listas..."
aws ec2 wait instance-running --instance-ids "${IDS[@]}"
aws ec2 wait instance-status-ok --instance-ids "${IDS[@]}"
escribir_hosts
echo; echo "Siguiente paso:  export SSH_KEY=<ruta a labsuser.pem> SSH_USER=ec2-user && bash deploy/desplegar.sh"
