from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import kafka_broker_expansion.kafka_checks as checks
from kafka_broker_expansion.config import KafkaConfig, MirrorMakerConfig
from kafka_broker_expansion.errors import HealthCheckError
from kafka_broker_expansion.kafka_checks import KafkaChecker


def metadata(*, brokers: int = 2, under_replicated: bool = False) -> SimpleNamespace:
    partition = SimpleNamespace(
        error=None,
        leader=0,
        replicas=[0, 1],
        isrs=[0] if under_replicated else [0, 1],
    )
    topic = SimpleNamespace(error=None, partitions={0: partition})
    return SimpleNamespace(
        brokers={number: object() for number in range(brokers)}, topics={"a": topic}
    )


def checker() -> KafkaChecker:
    return KafkaChecker(KafkaConfig("kafka:9092"), MirrorMakerConfig("source.heartbeats"))


def test_cluster_metadata_is_summarized(monkeypatch: pytest.MonkeyPatch) -> None:
    admin = Mock()
    admin.list_topics.return_value = metadata()
    instance = checker()
    monkeypatch.setattr(instance, "_admin", lambda: admin)

    health = instance.inspect_cluster(2)

    assert health.broker_ids == (0, 1)
    assert health.partition_count == 1
    assert health.under_replicated_partitions == 0


def test_cluster_rejects_wrong_broker_count(monkeypatch: pytest.MonkeyPatch) -> None:
    admin = Mock()
    admin.list_topics.return_value = metadata(brokers=1)
    instance = checker()
    monkeypatch.setattr(instance, "_admin", lambda: admin)

    with pytest.raises(HealthCheckError, match="expected 2"):
        instance.inspect_cluster(2)


def test_cluster_rejects_under_replicated_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    admin = Mock()
    admin.list_topics.return_value = metadata(under_replicated=True)
    instance = checker()
    monkeypatch.setattr(instance, "_admin", lambda: admin)

    with pytest.raises(HealthCheckError, match="under-replicated"):
        instance.inspect_cluster(2)


def test_recent_mirrormaker_heartbeat_is_healthy(monkeypatch: pytest.MonkeyPatch) -> None:
    admin = Mock()
    topic = SimpleNamespace(error=None, partitions={0: object()})
    admin.list_topics.return_value = SimpleNamespace(topics={"source.heartbeats": topic})
    instance = checker()
    monkeypatch.setattr(instance, "_admin", lambda: admin)
    consumer = Mock()
    consumer.get_watermark_offsets.return_value = (0, 1)
    message = Mock()
    message.error.return_value = None
    message.timestamp.return_value = (1, int(datetime.now(UTC).timestamp() * 1000))
    consumer.poll.return_value = message
    monkeypatch.setattr(checks, "Consumer", lambda _config: consumer)

    health = instance.inspect_mirrormaker_heartbeat()

    assert health.topic == "source.heartbeats"
    assert health.age_seconds < 2
    consumer.close.assert_called_once()


def test_empty_heartbeat_topic_is_unhealthy(monkeypatch: pytest.MonkeyPatch) -> None:
    admin = Mock()
    topic = SimpleNamespace(error=None, partitions={0: object()})
    admin.list_topics.return_value = SimpleNamespace(topics={"source.heartbeats": topic})
    instance = checker()
    monkeypatch.setattr(instance, "_admin", lambda: admin)
    consumer = Mock()
    consumer.get_watermark_offsets.return_value = (0, 0)
    monkeypatch.setattr(checks, "Consumer", lambda _config: consumer)

    with pytest.raises(HealthCheckError, match="no readable"):
        instance.inspect_mirrormaker_heartbeat()
