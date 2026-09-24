"""Single-purpose 2-to-3 broker expansion workflow."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from .config import ExpansionConfig
from .errors import ExpansionError, HealthCheckError
from .kafka_checks import KafkaChecker
from .kubernetes_checks import KubernetesChecker
from .logging import log_event
from .terraform import TerraformRunner, ValidatedPlan


class ExpansionWorkflow:
    def __init__(
        self,
        settings: ExpansionConfig,
        kafka: KafkaChecker,
        kubernetes: KubernetesChecker,
        terraform: TerraformRunner,
        logger: logging.Logger,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings
        self._kafka = kafka
        self._kubernetes = kubernetes
        self._terraform = terraform
        self._logger = logger
        self._sleep = sleep
        self._monotonic = monotonic

    def run(self, approve: Callable[[ValidatedPlan], bool]) -> None:
        """Run all checks and apply exactly one validated infrastructure change."""
        try:
            existing_broker_ids, new_broker_id = self._preflight()
            self._terraform.initialize()
            plan = self._terraform.plan_and_validate(self._settings.target_brokers)
            log_event(
                self._logger,
                logging.INFO,
                "terraform_plan_validated",
                "Terraform plan passed the change allow-list",
                resource=plan.resource_address,
                tfvars_file=plan.tfvars_path.name,
                before_replicas=plan.before_replicas,
                after_replicas=plan.after_replicas,
                new_broker_id=new_broker_id,
            )
            if not approve(plan):
                raise ExpansionError("operator declined the validated Terraform plan")

            log_event(
                self._logger,
                logging.INFO,
                "terraform_apply_started",
                "Applying validated Terraform plan",
            )
            self._terraform.apply(plan)
            self._wait_for_expansion(existing_broker_ids, new_broker_id)
            self._postflight()
            log_event(
                self._logger,
                logging.INFO,
                "expansion_succeeded",
                "Kafka broker expansion completed successfully",
                broker_count=self._settings.target_brokers,
            )
        finally:
            self._terraform.close()

    def _preflight(self) -> tuple[tuple[int, ...], int]:
        kafka = self._kafka.inspect_cluster(self._settings.expected_current_brokers)
        new_broker_id = max(kafka.broker_ids) + 1
        if kafka.broker_ids != (1, 2) or new_broker_id != 3:
            raise HealthCheckError(
                "V1 requires existing Kafka broker IDs [1, 2] so the new broker ID is 3; "
                f"discovered {list(kafka.broker_ids)}"
            )
        statefulset = self._kubernetes.inspect_statefulset(self._settings.expected_current_brokers)
        deployment = self._kubernetes.inspect_mirrormaker_workload()
        heartbeat = self._kafka.inspect_mirrormaker_heartbeat()
        log_event(
            self._logger,
            logging.INFO,
            "preflight_passed",
            "Kafka and MirrorMaker preflight checks passed",
            broker_ids=kafka.broker_ids,
            new_broker_id=new_broker_id,
            under_replicated_partitions=kafka.under_replicated_partitions,
            statefulset_ready=statefulset.ready_replicas,
            mirrormaker_ready=deployment.ready_replicas,
            heartbeat_age_seconds=round(heartbeat.age_seconds, 3),
        )
        return kafka.broker_ids, new_broker_id

    def _wait_for_expansion(self, existing_broker_ids: tuple[int, ...], new_broker_id: int) -> None:
        deadline = self._monotonic() + self._settings.timeout_seconds
        latest_error = "resources not checked"
        while self._monotonic() < deadline:
            try:
                resources = self._kubernetes.inspect_new_broker_resources(
                    self._settings.target_brokers - 1
                )
                statefulset = self._kubernetes.inspect_statefulset(self._settings.target_brokers)
                kafka = self._kafka.inspect_cluster(self._settings.target_brokers)
                expected_broker_ids = tuple(sorted((*existing_broker_ids, new_broker_id)))
                if kafka.broker_ids != expected_broker_ids:
                    raise HealthCheckError(
                        f"expected Kafka broker IDs {list(expected_broker_ids)}, "
                        f"found {list(kafka.broker_ids)}"
                    )
                log_event(
                    self._logger,
                    logging.INFO,
                    "new_broker_ready",
                    "New broker is registered and its Kubernetes resources are ready",
                    pod=resources.pod_name,
                    pvc=resources.pvc_name,
                    broker_ids=kafka.broker_ids,
                    statefulset_ready=statefulset.ready_replicas,
                )
                return
            except HealthCheckError as exc:
                latest_error = str(exc)
                log_event(
                    self._logger,
                    logging.DEBUG,
                    "new_broker_waiting",
                    "Waiting for the new broker to become ready",
                    reason=latest_error,
                )
                self._sleep(self._settings.poll_interval_seconds)
        raise HealthCheckError(
            f"new broker did not become ready within {self._settings.timeout_seconds}s: "
            f"{latest_error}. Terraform was applied; no automatic rollback was attempted"
        )

    def _postflight(self) -> None:
        deployment = self._kubernetes.inspect_mirrormaker_workload()
        heartbeat = self._kafka.inspect_mirrormaker_heartbeat()
        log_event(
            self._logger,
            logging.INFO,
            "postflight_passed",
            "MirrorMaker remained healthy after expansion",
            mirrormaker_ready=deployment.ready_replicas,
            heartbeat_age_seconds=round(heartbeat.age_seconds, 3),
        )
