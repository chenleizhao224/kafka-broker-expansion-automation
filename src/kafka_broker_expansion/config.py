"""Configuration loading and V1 scope validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .errors import ConfigurationError


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    request_timeout_seconds: float = 10.0
    require_fully_replicated_partitions: bool = True


@dataclass(frozen=True)
class KubernetesConfig:
    namespace: str
    statefulset: str
    broker_container: str
    data_claim_name: str
    mirrormaker_deployment: str
    context: str | None = None


@dataclass(frozen=True)
class MirrorMakerConfig:
    heartbeat_topic: str
    max_heartbeat_age_seconds: int = 120


@dataclass(frozen=True)
class TerraformConfig:
    directory: Path
    binary: str = "terraform"
    statefulset_address: str = "kubernetes_stateful_set_v1.kafka"


@dataclass(frozen=True)
class ExpansionConfig:
    expected_current_brokers: int
    target_brokers: int
    timeout_seconds: int
    poll_interval_seconds: float
    kafka: KafkaConfig
    kubernetes: KubernetesConfig
    mirrormaker: MirrorMakerConfig
    terraform: TerraformConfig


def _mapping(value: Any, key: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"'{key}' must be a mapping")
    return value


def _required(data: dict[str, Any], key: str) -> Any:
    if key not in data or data[key] in (None, ""):
        raise ConfigurationError(f"missing required configuration value: {key}")
    return data[key]


def load_config(path: Path) -> ExpansionConfig:
    """Load YAML configuration and enforce the deliberately narrow V1 operation."""
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigurationError(f"cannot read configuration {path}: {exc}") from exc

    root = _mapping(raw, "root")
    kafka_raw = _mapping(_required(root, "kafka"), "kafka")
    kube_raw = _mapping(_required(root, "kubernetes"), "kubernetes")
    mm_raw = _mapping(_required(root, "mirrormaker"), "mirrormaker")
    tf_raw = _mapping(_required(root, "terraform"), "terraform")

    expected = int(root.get("expected_current_brokers", 2))
    target = int(root.get("target_brokers", 3))
    if (expected, target) != (2, 3):
        raise ConfigurationError("V1 only supports one expansion: exactly 2 brokers to 3")

    timeout = int(root.get("timeout_seconds", 600))
    poll = float(root.get("poll_interval_seconds", 5))
    if timeout <= 0 or poll <= 0 or poll > timeout:
        raise ConfigurationError("timeout and poll interval must be positive, with poll <= timeout")

    config_dir = path.resolve().parent
    tf_directory = Path(str(_required(tf_raw, "directory")))
    if not tf_directory.is_absolute():
        tf_directory = (config_dir / tf_directory).resolve()

    return ExpansionConfig(
        expected_current_brokers=expected,
        target_brokers=target,
        timeout_seconds=timeout,
        poll_interval_seconds=poll,
        kafka=KafkaConfig(
            bootstrap_servers=str(_required(kafka_raw, "bootstrap_servers")),
            request_timeout_seconds=float(kafka_raw.get("request_timeout_seconds", 10)),
            require_fully_replicated_partitions=bool(
                kafka_raw.get("require_fully_replicated_partitions", True)
            ),
        ),
        kubernetes=KubernetesConfig(
            namespace=str(_required(kube_raw, "namespace")),
            statefulset=str(_required(kube_raw, "statefulset")),
            broker_container=str(kube_raw.get("broker_container", "kafka")),
            data_claim_name=str(kube_raw.get("data_claim_name", "data")),
            mirrormaker_deployment=str(_required(kube_raw, "mirrormaker_deployment")),
            context=str(kube_raw["context"]) if kube_raw.get("context") else None,
        ),
        mirrormaker=MirrorMakerConfig(
            heartbeat_topic=str(_required(mm_raw, "heartbeat_topic")),
            max_heartbeat_age_seconds=int(mm_raw.get("max_heartbeat_age_seconds", 120)),
        ),
        terraform=TerraformConfig(
            directory=tf_directory,
            binary=str(tf_raw.get("binary", "terraform")),
            statefulset_address=str(
                tf_raw.get("statefulset_address", "kubernetes_stateful_set_v1.kafka")
            ),
        ),
    )
