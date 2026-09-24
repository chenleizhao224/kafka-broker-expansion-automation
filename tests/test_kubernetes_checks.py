from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from kafka_broker_expansion.config import KubernetesConfig
from kafka_broker_expansion.errors import HealthCheckError
from kafka_broker_expansion.kubernetes_checks import KubernetesChecker


def checker() -> KubernetesChecker:
    instance = object.__new__(KubernetesChecker)
    instance._settings = KubernetesConfig("kafka", "kafka", "kafka", "data", "mm2")
    instance._apps = Mock()
    instance._core = Mock()
    return instance


def workload(*, replicas: int = 2, ready: int = 2) -> SimpleNamespace:
    return SimpleNamespace(
        spec=SimpleNamespace(replicas=replicas),
        status=SimpleNamespace(ready_replicas=ready, observed_generation=2, updated_replicas=ready),
        metadata=SimpleNamespace(generation=2),
    )


def test_statefulset_must_be_settled_at_expected_count() -> None:
    instance = checker()
    instance._apps.read_namespaced_stateful_set.return_value = workload()

    health = instance.inspect_statefulset(2)

    assert health.replicas == 2
    assert health.ready_replicas == 2


def test_statefulset_rejects_unready_replica() -> None:
    instance = checker()
    instance._apps.read_namespaced_stateful_set.return_value = workload(ready=1)

    with pytest.raises(HealthCheckError, match="not settled"):
        instance.inspect_statefulset(2)


def test_mirrormaker_deployment_must_be_fully_rolled_out() -> None:
    instance = checker()
    instance._apps.read_namespaced_deployment.return_value = workload(replicas=1, ready=1)

    health = instance.inspect_mirrormaker_workload()

    assert health.desired_replicas == health.ready_replicas == 1


def test_new_broker_pod_and_pvc_are_ready() -> None:
    instance = checker()
    instance._core.read_namespaced_pod.return_value = SimpleNamespace(
        status=SimpleNamespace(
            conditions=[SimpleNamespace(type="Ready", status="True")],
            container_statuses=[SimpleNamespace(name="kafka", ready=True)],
        )
    )
    instance._core.read_namespaced_persistent_volume_claim.return_value = SimpleNamespace(
        status=SimpleNamespace(phase="Bound")
    )

    resources = instance.inspect_new_broker_resources(2)

    assert resources.pod_name == "kafka-2"
    assert resources.pvc_name == "data-kafka-2"


def test_new_broker_rejects_pending_pvc() -> None:
    instance = checker()
    instance._core.read_namespaced_pod.return_value = SimpleNamespace(
        status=SimpleNamespace(
            conditions=[SimpleNamespace(type="Ready", status="True")],
            container_statuses=[SimpleNamespace(name="kafka", ready=True)],
        )
    )
    instance._core.read_namespaced_persistent_volume_claim.return_value = SimpleNamespace(
        status=SimpleNamespace(phase="Pending")
    )

    with pytest.raises(HealthCheckError, match="not Bound"):
        instance.inspect_new_broker_resources(2)
