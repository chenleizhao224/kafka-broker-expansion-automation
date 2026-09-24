from pathlib import Path

import pytest

from kafka_broker_expansion.config import load_config
from kafka_broker_expansion.errors import ConfigurationError


def write_config(path: Path, *, expected: int = 2, target: int = 3) -> None:
    path.write_text(
        f"""
expected_current_brokers: {expected}
target_brokers: {target}
timeout_seconds: 30
poll_interval_seconds: 1
kafka:
  bootstrap_servers: kafka:9092
kubernetes:
  namespace: kafka
  statefulset: kafka
  mirrormaker_deployment: mm2
mirrormaker:
  heartbeat_topic: source.heartbeats
terraform:
  directory: ../terraform
""",
        encoding="utf-8",
    )


def test_loads_config_and_resolves_terraform_path(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    write_config(config_path)

    config = load_config(config_path)

    assert config.expected_current_brokers == 2
    assert config.target_brokers == 3
    assert config.kafka.bootstrap_servers == "kafka:9092"
    assert config.kubernetes.data_claim_name == "data"
    assert config.terraform.directory == (tmp_path / "../terraform").resolve()


@pytest.mark.parametrize(("expected", "target"), [(1, 2), (2, 4), (3, 2)])
def test_rejects_operations_outside_v1_scope(tmp_path: Path, expected: int, target: int) -> None:
    config_path = tmp_path / "config.yaml"
    write_config(config_path, expected=expected, target=target)

    with pytest.raises(ConfigurationError, match="exactly 2 brokers to 3"):
        load_config(config_path)


def test_rejects_missing_required_section(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("expected_current_brokers: 2\n", encoding="utf-8")

    with pytest.raises(ConfigurationError, match="missing required"):
        load_config(config_path)
