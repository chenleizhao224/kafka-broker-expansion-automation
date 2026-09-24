"""Kafka metadata and MirrorMaker heartbeat health checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from .config import KafkaConfig, MirrorMakerConfig
from .errors import HealthCheckError


@dataclass(frozen=True)
class KafkaHealth:
    broker_ids: tuple[int, ...]
    topic_count: int
    partition_count: int
    under_replicated_partitions: int


@dataclass(frozen=True)
class HeartbeatHealth:
    topic: str
    newest_timestamp: datetime
    age_seconds: float


class KafkaChecker:
    """Perform read-only checks against Kafka and MM2's heartbeat topic."""

    def __init__(self, kafka: KafkaConfig, mirrormaker: MirrorMakerConfig) -> None:
        self._config = kafka
        self._mirrormaker = mirrormaker

    def inspect_cluster(self, expected_brokers: int) -> KafkaHealth:
        metadata = self._admin().list_topics(timeout=self._config.request_timeout_seconds)
        if metadata.brokers is None:
            raise HealthCheckError("Kafka returned no broker metadata")
        broker_ids = tuple(sorted(int(broker_id) for broker_id in metadata.brokers))
        if len(broker_ids) != expected_brokers:
            raise HealthCheckError(
                f"expected {expected_brokers} Kafka brokers, found {len(broker_ids)}: {broker_ids}"
            )

        partitions = 0
        under_replicated = 0
        for topic_name, topic in metadata.topics.items():
            if topic.error is not None:
                raise HealthCheckError(
                    f"Kafka topic metadata error for {topic_name}: {topic.error}"
                )
            for partition_id, partition in topic.partitions.items():
                partitions += 1
                if partition.error is not None or partition.leader < 0:
                    raise HealthCheckError(
                        f"unavailable Kafka partition {topic_name}[{partition_id}]"
                    )
                if len(partition.isrs) < len(partition.replicas):
                    under_replicated += 1

        if self._config.require_fully_replicated_partitions and under_replicated:
            raise HealthCheckError(f"Kafka has {under_replicated} under-replicated partitions")

        return KafkaHealth(
            broker_ids=broker_ids,
            topic_count=len(metadata.topics),
            partition_count=partitions,
            under_replicated_partitions=under_replicated,
        )

    def inspect_mirrormaker_heartbeat(self) -> HeartbeatHealth:
        """Require a recent record in the MM2 heartbeat topic on the target cluster."""
        topic_name = self._mirrormaker.heartbeat_topic
        metadata = self._admin().list_topics(
            topic=topic_name, timeout=self._config.request_timeout_seconds
        )
        topic = metadata.topics.get(topic_name)
        if topic is None or topic.error is not None:
            raise HealthCheckError(f"MirrorMaker heartbeat topic is unavailable: {topic_name}")

        consumer = Consumer(
            {
                "bootstrap.servers": self._config.bootstrap_servers,
                "group.id": f"kafka-expansion-health-{uuid4()}",
                "enable.auto.commit": False,
                "enable.partition.eof": False,
            }
        )
        newest_ms: int | None = None
        try:
            for partition_id in topic.partitions:
                probe = TopicPartition(topic_name, partition_id)
                _low, high = consumer.get_watermark_offsets(
                    probe, timeout=self._config.request_timeout_seconds, cached=False
                )
                if high <= 0:
                    continue
                consumer.assign([TopicPartition(topic_name, partition_id, high - 1)])
                message = consumer.poll(self._config.request_timeout_seconds)
                if message is None or message.error() is not None:
                    continue
                _timestamp_type, timestamp_ms = message.timestamp()
                if timestamp_ms is not None and (newest_ms is None or timestamp_ms > newest_ms):
                    newest_ms = int(timestamp_ms)
        finally:
            consumer.close()

        if newest_ms is None:
            raise HealthCheckError(f"no readable MirrorMaker heartbeat in {topic_name}")
        newest = datetime.fromtimestamp(newest_ms / 1000, tz=UTC)
        age = (datetime.now(UTC) - newest).total_seconds()
        if age < -30:
            raise HealthCheckError("MirrorMaker heartbeat timestamp is unexpectedly in the future")
        if age > self._mirrormaker.max_heartbeat_age_seconds:
            raise HealthCheckError(
                f"MirrorMaker heartbeat is stale ({age:.1f}s old; "
                f"limit {self._mirrormaker.max_heartbeat_age_seconds}s)"
            )
        return HeartbeatHealth(topic=topic_name, newest_timestamp=newest, age_seconds=age)

    def _admin(self) -> Any:
        return AdminClient(
            {
                "bootstrap.servers": self._config.bootstrap_servers,
                "socket.timeout.ms": int(self._config.request_timeout_seconds * 1000),
            }
        )
