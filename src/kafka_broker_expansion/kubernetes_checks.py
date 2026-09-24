"""Read-only Kubernetes readiness checks used around the expansion."""

from __future__ import annotations

from dataclasses import dataclass

from kubernetes import client, config  # type: ignore[import-untyped]
from kubernetes.client import ApiException  # type: ignore[import-untyped]

from .config import KubernetesConfig
from .errors import HealthCheckError


@dataclass(frozen=True)
class StatefulSetHealth:
    replicas: int
    ready_replicas: int
    observed_generation: int


@dataclass(frozen=True)
class MirrorMakerWorkloadHealth:
    desired_replicas: int
    ready_replicas: int
    observed_generation: int


@dataclass(frozen=True)
class NewBrokerResources:
    pod_name: str
    pvc_name: str


class KubernetesChecker:
    def __init__(self, settings: KubernetesConfig) -> None:
        self._settings = settings
        try:
            config.load_kube_config(context=settings.context)
        except Exception as exc:
            raise HealthCheckError(f"cannot load Kubernetes configuration: {exc}") from exc
        self._apps = client.AppsV1Api()
        self._core = client.CoreV1Api()

    def inspect_statefulset(self, expected_replicas: int) -> StatefulSetHealth:
        try:
            workload = self._apps.read_namespaced_stateful_set(
                self._settings.statefulset, self._settings.namespace
            )
        except ApiException as exc:
            raise HealthCheckError(f"cannot read Kafka StatefulSet: {exc.reason}") from exc

        desired = int(workload.spec.replicas or 0)
        ready = int(workload.status.ready_replicas or 0)
        observed = int(workload.status.observed_generation or 0)
        generation = int(workload.metadata.generation or 0)
        if desired != expected_replicas:
            raise HealthCheckError(
                f"StatefulSet expects {desired} replicas, not {expected_replicas}"
            )
        if ready != expected_replicas or observed < generation:
            raise HealthCheckError(
                f"Kafka StatefulSet is not settled: desired={desired}, ready={ready}, "
                f"generation={generation}, observed={observed}"
            )
        return StatefulSetHealth(desired, ready, observed)

    def inspect_mirrormaker_workload(self) -> MirrorMakerWorkloadHealth:
        try:
            deployment = self._apps.read_namespaced_deployment(
                self._settings.mirrormaker_deployment, self._settings.namespace
            )
        except ApiException as exc:
            raise HealthCheckError(f"cannot read MirrorMaker Deployment: {exc.reason}") from exc

        desired = int(deployment.spec.replicas or 0)
        ready = int(deployment.status.ready_replicas or 0)
        updated = int(deployment.status.updated_replicas or 0)
        observed = int(deployment.status.observed_generation or 0)
        generation = int(deployment.metadata.generation or 0)
        if desired < 1 or ready != desired or updated != desired or observed < generation:
            raise HealthCheckError(
                "MirrorMaker Deployment is not fully rolled out: "
                f"desired={desired}, ready={ready}, updated={updated}, "
                f"generation={generation}, observed={observed}"
            )
        return MirrorMakerWorkloadHealth(desired, ready, observed)

    def inspect_new_broker_resources(self, ordinal: int) -> NewBrokerResources:
        pod_name = f"{self._settings.statefulset}-{ordinal}"
        pvc_name = f"{self._settings.data_claim_name}-{pod_name}"
        try:
            pod = self._core.read_namespaced_pod(pod_name, self._settings.namespace)
            pvc = self._core.read_namespaced_persistent_volume_claim(
                pvc_name, self._settings.namespace
            )
        except ApiException as exc:
            raise HealthCheckError(
                f"new broker resources are not readable yet: {exc.reason}"
            ) from exc

        ready_condition = next(
            (condition for condition in (pod.status.conditions or []) if condition.type == "Ready"),
            None,
        )
        if ready_condition is None or ready_condition.status != "True":
            raise HealthCheckError(f"new broker pod is not Ready: {pod_name}")
        containers = pod.status.container_statuses or []
        broker_status = next(
            (status for status in containers if status.name == self._settings.broker_container),
            None,
        )
        if broker_status is None or not broker_status.ready:
            raise HealthCheckError(
                f"broker container {self._settings.broker_container} is not Ready in {pod_name}"
            )
        if pvc.status.phase != "Bound":
            raise HealthCheckError(f"new broker PVC is not Bound: {pvc_name}")
        return NewBrokerResources(pod_name=pod_name, pvc_name=pvc_name)
