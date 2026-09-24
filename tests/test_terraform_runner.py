import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from kafka_broker_expansion.config import TerraformConfig
from kafka_broker_expansion.errors import CommandError
from kafka_broker_expansion.terraform import TerraformRunner
from tests.test_terraform_plan import safe_plan


def test_runner_builds_validates_and_applies_saved_plan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    commands: list[list[str]] = []

    def run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        commands.append(command)
        stdout = json.dumps(safe_plan()) if "show" in command else ""
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    monkeypatch.setattr("kafka_broker_expansion.terraform.subprocess.run", run)
    runner = TerraformRunner(TerraformConfig(tmp_path))

    runner.initialize()
    plan = runner.plan_and_validate(3)
    runner.apply(plan)

    assert plan.before_replicas == 2
    assert json.loads(plan.tfvars_path.read_text(encoding="utf-8")) == {"broker_count": 3}
    assert any(any(part.startswith("-var-file=") for part in command) for command in commands)
    assert any("plan" in command for command in commands)
    assert any("apply" in command for command in commands)
    runner.close()


def test_runner_surfaces_command_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "kafka_broker_expansion.terraform.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout="", stderr="bad plan"),
    )
    runner = TerraformRunner(TerraformConfig(tmp_path))

    with pytest.raises(CommandError, match="bad plan"):
        runner.initialize()
