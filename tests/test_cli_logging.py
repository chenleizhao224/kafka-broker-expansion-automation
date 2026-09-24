import json
import logging
from pathlib import Path
from unittest.mock import Mock

import pytest

import kafka_broker_expansion.cli as cli
from kafka_broker_expansion.errors import ExpansionError
from kafka_broker_expansion.logging import JsonFormatter
from kafka_broker_expansion.terraform import ValidatedPlan


def test_json_formatter_emits_event_and_fields() -> None:
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "hello", (), None)
    record.event = "checked"  # type: ignore[attr-defined]
    record.fields = {"brokers": 3}  # type: ignore[attr-defined]

    payload = json.loads(JsonFormatter().format(record))

    assert payload["event"] == "checked"
    assert payload["brokers"] == 3


def test_confirmation_auto_approves_and_interactive_defaults_no(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = ValidatedPlan(Path("plan"), Path("tfvars.json"), "resource", 2, 3)
    assert cli._confirmation(True)(plan)
    monkeypatch.setattr("builtins.input", lambda _prompt: "")
    assert not cli._confirmation(False)(plan)


def test_main_runs_workflow(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Mock()
    workflow = Mock()
    monkeypatch.setattr(cli, "load_config", lambda _path: settings)
    monkeypatch.setattr(cli, "KafkaChecker", Mock())
    monkeypatch.setattr(cli, "KubernetesChecker", Mock())
    monkeypatch.setattr(cli, "TerraformRunner", Mock())
    monkeypatch.setattr(cli, "ExpansionWorkflow", Mock(return_value=workflow))

    assert cli.main(["--config", "config.yaml", "--yes"]) == 0
    workflow.run.assert_called_once()


def test_main_returns_failure_for_domain_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "load_config", Mock(side_effect=ExpansionError("stop")))

    assert cli.main(["--config", "bad.yaml"]) == 1
