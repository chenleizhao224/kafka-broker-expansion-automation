#!/usr/bin/env bash
set -Eeo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="kafka-expansion-demo"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="kafka"
TARGET_CLUSTER_ID="MkU3OEVBNTcwNTJENDM2Qk"
PORT_FORWARD_PIDS=()
export KUBE_CONFIG_PATH="${HOME}/.kube/config"

cd "${PROJECT_DIR}"
mkdir -p work

cleanup() {
  for pid in "${PORT_FORWARD_PIDS[@]}"; do
    kill "${pid}" 2>/dev/null || true
  done
}
trap cleanup EXIT

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "缺少 $1，暂时不能运行演示。"
    exit 1
  fi
}

for command in docker kind kubectl terraform; do
  require_command "${command}"
done

if [[ ! -x .venv/bin/kafka-expand ]]; then
  echo "请先运行：source .venv/bin/activate && make install"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Docker 没有运行。请先打开 Docker Desktop，等它启动后再运行 make demo。"
  exit 1
fi

if ! kind get clusters | grep -qx "${CLUSTER_NAME}"; then
  echo "[1/7] 创建隔离的本地 Kubernetes 集群…"
  kind create cluster --config demo/kind.yaml
else
  echo "[1/7] 使用已有的本地演示集群。"
fi

kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" scale \
  deployment/kafka-mirrormaker2 --replicas=0 >/dev/null 2>&1 || true

echo "[2/7] 启动 Kafka controller…"
kubectl --context "${KUBE_CONTEXT}" apply --validate=false \
  -f demo/kafka-foundation.yaml >/dev/null
kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" rollout status \
  statefulset/kafka-controller --timeout=5m

cp demo/terraform.tfvars terraform/terraform.tfvars
export TF_VAR_cluster_id="${TARGET_CLUSTER_ID}"

echo "[3/7] 用 Terraform 创建两个目标 broker…"
terraform -chdir=terraform init -backend=false -input=false >/dev/null
current_replicas="$(kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" \
  get statefulset kafka -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
if [[ "${current_replicas}" == "3" ]]; then
  echo "这个演示已经完成过：StatefulSet 当前是 3 个 broker。"
  echo "如需重新演示，请新建一个干净环境后再运行。"
  exit 0
fi
terraform -chdir=terraform apply -input=false -auto-approve -var=broker_count=2 >/dev/null
kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" rollout status \
  statefulset/kafka --timeout=5m

echo "[4/7] 启动 MirrorMaker heartbeat 模拟器…"
kubectl --context "${KUBE_CONTEXT}" apply --validate=false -f demo/mirrormaker.yaml >/dev/null
kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" rollout restart \
  deployment/kafka-mirrormaker2 >/dev/null
kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" rollout status \
  deployment/kafka-mirrormaker2 --timeout=5m

echo "[5/7] 连接本地检查端口…"
kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" port-forward \
  pod/kafka-0 19092:9094 >work/port-forward-kafka-0.log 2>&1 &
PORT_FORWARD_PIDS+=("$!")
kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" port-forward \
  pod/kafka-1 19093:9094 >work/port-forward-kafka-1.log 2>&1 &
PORT_FORWARD_PIDS+=("$!")
(
  until kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" get pod kafka-2 \
    >/dev/null 2>&1; do
    sleep 2
  done
  exec kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" port-forward \
    pod/kafka-2 19094:9094 >work/port-forward-kafka-2.log 2>&1
) &
PORT_FORWARD_PIDS+=("$!")

for _attempt in $(seq 1 30); do
  if .venv/bin/python -c \
    'import socket; s=socket.create_connection(("127.0.0.1", 19092), 1); s.close()' \
    >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[6/7] 等待 MirrorMaker heartbeat…"
heartbeat_ready=false
for _attempt in $(seq 1 36); do
  if .venv/bin/python -c \
    'from pathlib import Path; from kafka_broker_expansion.config import load_config; from kafka_broker_expansion.kafka_checks import KafkaChecker; c=load_config(Path("config/demo.yaml")); KafkaChecker(c.kafka,c.mirrormaker).inspect_mirrormaker_heartbeat()' \
    >/dev/null 2>&1; then
    heartbeat_ready=true
    break
  fi
  sleep 5
done
if [[ "${heartbeat_ready}" != "true" ]]; then
  echo "MirrorMaker heartbeat 没有就绪。日志："
  kubectl --context "${KUBE_CONTEXT}" -n "${NAMESPACE}" logs \
    deployment/kafka-mirrormaker2 --tail=40
  exit 1
fi

echo "[7/7] 执行真实的 2 → 3 broker 扩容…"
.venv/bin/kafka-expand --config config/demo.yaml --yes

echo
echo "演示成功：Kafka broker 已从 [1, 2] 扩容到 [1, 2, 3]。"
