"""Terraform execution and strict plan allow-list validation."""

from __future__ import annotations

import json
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import TerraformConfig
from .errors import CommandError, UnsafePlanError


@dataclass(frozen=True)
class ValidatedPlan:
    path: Path
    tfvars_path: Path
    resource_address: str
    before_replicas: int
    after_replicas: int


def _replicas(value: Any) -> int | None:
    if not isinstance(value, dict):
        return None
    spec = value.get("spec")
    if not isinstance(spec, list) or not spec or not isinstance(spec[0], dict):
        return None
    replicas = spec[0].get("replicas")
    return int(replicas) if replicas is not None else None


def validate_plan_json(
    plan: dict[str, Any], *, expected_address: str, before: int = 2, after: int = 3
) -> tuple[int, int]:
    """Allow only an in-place 2 -> 3 replica update on the Kafka StatefulSet."""
    changes = plan.get("resource_changes")
    if not isinstance(changes, list):
        raise UnsafePlanError("Terraform plan has no resource_changes array")

    meaningful: list[dict[str, Any]] = []
    for item in changes:
        if not isinstance(item, dict):
            raise UnsafePlanError("Terraform returned a malformed resource change")
        change = item.get("change")
        actions = change.get("actions") if isinstance(change, dict) else None
        if actions in (["no-op"], ["read"]):
            continue
        meaningful.append(item)

    if len(meaningful) != 1:
        addresses = [str(item.get("address", "<unknown>")) for item in meaningful]
        raise UnsafePlanError(
            f"expected exactly one changed resource, found {len(meaningful)}: {addresses}"
        )

    item = meaningful[0]
    address = str(item.get("address", ""))
    change = item.get("change")
    if address != expected_address or not isinstance(change, dict):
        raise UnsafePlanError(f"only {expected_address} may change; found {address or '<unknown>'}")

    actions = change.get("actions")
    if actions != ["update"]:
        raise UnsafePlanError(f"StatefulSet change must be in-place update only; actions={actions}")
    if change.get("replace_paths"):
        raise UnsafePlanError("StatefulSet replacement paths are not allowed")

    actual_before = _replicas(change.get("before"))
    actual_after = _replicas(change.get("after"))
    if (actual_before, actual_after) != (before, after):
        raise UnsafePlanError(
            f"only replica count {before} -> {after} is allowed; plan has "
            f"{actual_before} -> {actual_after}"
        )

    before_value = change.get("before")
    after_value = change.get("after")
    if not isinstance(before_value, dict) or not isinstance(after_value, dict):
        raise UnsafePlanError("StatefulSet before/after values are missing")
    normalized_before = json.loads(json.dumps(before_value))
    normalized_after = json.loads(json.dumps(after_value))
    normalized_before["spec"][0]["replicas"] = after
    if normalized_before != normalized_after:
        raise UnsafePlanError("plan changes StatefulSet fields other than replicas")
    return before, after


class TerraformRunner:
    def __init__(self, settings: TerraformConfig) -> None:
        self._settings = settings
        self._temporary_directory: tempfile.TemporaryDirectory[str] | None = None

    def initialize(self) -> None:
        self._run("init", "-input=false", "-no-color")

    def plan_and_validate(self, target_brokers: int) -> ValidatedPlan:
        self._temporary_directory = tempfile.TemporaryDirectory(prefix="kafka-expansion-")
        temporary_path = Path(self._temporary_directory.name)
        plan_path = temporary_path / "expansion.tfplan"
        tfvars_path = temporary_path / "expansion.tfvars.json"
        tfvars_path.write_text(
            json.dumps({"broker_count": target_brokers}, indent=2) + "\n",
            encoding="utf-8",
        )
        self._run(
            "plan",
            "-input=false",
            "-no-color",
            f"-var-file={tfvars_path}",
            f"-out={plan_path}",
        )
        raw = self._run("show", "-json", str(plan_path))
        try:
            plan = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CommandError(f"terraform show returned invalid JSON: {exc}") from exc
        before, after = validate_plan_json(
            plan,
            expected_address=self._settings.statefulset_address,
            before=2,
            after=target_brokers,
        )
        return ValidatedPlan(
            plan_path,
            tfvars_path,
            self._settings.statefulset_address,
            before,
            after,
        )

    def apply(self, plan: ValidatedPlan) -> None:
        self._run("apply", "-input=false", "-no-color", "-auto-approve", str(plan.path))

    def close(self) -> None:
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()
            self._temporary_directory = None

    def _run(self, *args: str) -> str:
        command = [self._settings.binary, f"-chdir={self._settings.directory}", *args]
        try:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=900,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise CommandError(f"cannot run {' '.join(command[:3])}: {exc}") from exc
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise CommandError(f"{' '.join(command[:3])} failed: {detail}")
        return result.stdout
