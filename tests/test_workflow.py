import logging
from pathlib import Path
from unittest.mock import Mock, call

import pytest

from kafka_broker_expansion.config import (
    ExpansionConfig,
    KafkaConfig,
    KubernetesConfig,
    MirrorMakerConfig,
    TerraformConfig,
)
from kafka_broker_expansion.errors import ExpansionError, HealthCheckError
from kafka_broker_expansion.kafka_checks import HeartbeatHealth, KafkaHealth
from kafka_broker_expansion.kubernetes_checks import (
    MirrorMakerWorkloadHealth,
    NewBrokerResources,
    StatefulSetHealth,
)
from kafka_broker_expansion.terraform import ValidatedPlan
from kafka_broker_expansion.workflow import ExpansionWorkflow


def settings() -> ExpansionConfig:
    return ExpansionConfig(
        expected_current_brokers=2,
        target_brokers=3,
        timeout_seconds=10,
        poll_interval_seconds=1,
        kafka=KafkaConfig("kafka:9092"),
        kubernetes=KubernetesConfig("kafka", "kafka", "kafka", "data", "mm2"),
        mirrormaker=MirrorMakerConfig("source.heartbeats"),
        terraform=TerraformConfig(Path("terraform")),
    )


def dependencies() -> tuple[Mock, Mock, Mock]:
    kafka = Mock()
    kafka.inspect_cluster.side_effect = [
        KafkaHealth((0, 1), 2, 6, 0),
        KafkaHealth((0, 1, 2), 2, 6, 0),
    ]
    kafka.inspect_mirrormaker_heartbeat.return_value = HeartbeatHealth(
        "source.heartbeats", Mock(), 1.5
    )
    kube = Mock()
    kube.inspect_statefulset.side_effect = [
        StatefulSetHealth(2, 2, 1),
        StatefulSetHealth(3, 3, 2),
    ]
    kube.inspect_mirrormaker_workload.return_value = MirrorMakerWorkloadHealth(1, 1, 1)
    kube.inspect_new_broker_resources.return_value = NewBrokerResources("kafka-2", "data-kafka-2")
    terraform = Mock()
    terraform.plan_and_validate.return_value = ValidatedPlan(
        Path("plan"), "kubernetes_stateful_set_v1.kafka", 2, 3
    )
    return kafka, kube, terraform


def test_runs_checks_plan_apply_wait_and_postcheck_in_order() -> None:
    kafka, kube, terraform = dependencies()
    workflow = ExpansionWorkflow(settings(), kafka, kube, terraform, logging.getLogger("test"))

    workflow.run(lambda _plan: True)

    terraform.assert_has_calls(
        [
            call.initialize(),
            call.plan_and_validate(3),
            call.apply(terraform.plan_and_validate.return_value),
        ]
    )
    assert kafka.inspect_cluster.call_args_list == [call(2), call(3)]
    assert kafka.inspect_mirrormaker_heartbeat.call_count == 2
    assert kube.inspect_mirrormaker_workload.call_count == 2
    terraform.close.assert_called_once()


def test_declined_plan_is_never_applied() -> None:
    kafka, kube, terraform = dependencies()
    workflow = ExpansionWorkflow(settings(), kafka, kube, terraform, logging.getLogger("test"))

    with pytest.raises(ExpansionError, match="declined"):
        workflow.run(lambda _plan: False)

    terraform.apply.assert_not_called()
    terraform.close.assert_called_once()


def test_timeout_reports_no_automatic_rollback() -> None:
    kafka, kube, terraform = dependencies()
    kube.inspect_new_broker_resources.side_effect = HealthCheckError("pod not ready")
    ticks = iter([0.0, 0.0, 5.0, 10.0])
    workflow = ExpansionWorkflow(
        settings(),
        kafka,
        kube,
        terraform,
        logging.getLogger("test"),
        sleep=lambda _seconds: None,
        monotonic=lambda: next(ticks),
    )

    with pytest.raises(HealthCheckError, match="no automatic rollback"):
        workflow.run(lambda _plan: True)

    terraform.apply.assert_called_once()
    terraform.close.assert_called_once()
