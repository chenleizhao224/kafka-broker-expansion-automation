from copy import deepcopy

import pytest

from kafka_broker_expansion.errors import UnsafePlanError
from kafka_broker_expansion.terraform import validate_plan_json

ADDRESS = "kubernetes_stateful_set_v1.kafka"


def state(replicas: int) -> dict[str, object]:
    return {
        "metadata": [{"name": "kafka", "namespace": "kafka"}],
        "spec": [{"replicas": replicas, "service_name": "kafka"}],
    }


def safe_plan() -> dict[str, object]:
    return {
        "resource_changes": [
            {
                "address": "kubernetes_service_v1.kafka_headless",
                "change": {"actions": ["no-op"], "before": {}, "after": {}},
            },
            {
                "address": ADDRESS,
                "change": {
                    "actions": ["update"],
                    "before": state(2),
                    "after": state(3),
                    "replace_paths": [],
                },
            },
        ]
    }


def test_accepts_only_replica_scale_up() -> None:
    assert validate_plan_json(safe_plan(), expected_address=ADDRESS) == (2, 3)


@pytest.mark.parametrize("actions", [["delete"], ["create", "delete"], ["create"]])
def test_rejects_destructive_or_creating_actions(actions: list[str]) -> None:
    plan = safe_plan()
    plan["resource_changes"][1]["change"]["actions"] = actions  # type: ignore[index]

    with pytest.raises(UnsafePlanError, match="in-place update"):
        validate_plan_json(plan, expected_address=ADDRESS)


def test_rejects_changes_to_another_resource() -> None:
    plan = safe_plan()
    plan["resource_changes"][1]["address"] = "kubernetes_service_v1.kafka"  # type: ignore[index]

    with pytest.raises(UnsafePlanError, match=r"only kubernetes_stateful_set_v1\.kafka"):
        validate_plan_json(plan, expected_address=ADDRESS)


def test_rejects_any_second_meaningful_change() -> None:
    plan = safe_plan()
    plan["resource_changes"].append(  # type: ignore[union-attr]
        {
            "address": "kubernetes_persistent_volume_claim.bad",
            "change": {"actions": ["delete"], "before": {}, "after": None},
        }
    )

    with pytest.raises(UnsafePlanError, match="exactly one changed resource"):
        validate_plan_json(plan, expected_address=ADDRESS)


def test_rejects_non_replica_field_change() -> None:
    plan = safe_plan()
    unsafe = deepcopy(state(3))
    unsafe["spec"][0]["service_name"] = "different"  # type: ignore[index]
    plan["resource_changes"][1]["change"]["after"] = unsafe  # type: ignore[index]

    with pytest.raises(UnsafePlanError, match="fields other than replicas"):
        validate_plan_json(plan, expected_address=ADDRESS)


def test_rejects_wrong_replica_delta() -> None:
    plan = safe_plan()
    plan["resource_changes"][1]["change"]["after"] = state(4)  # type: ignore[index]

    with pytest.raises(UnsafePlanError, match="2 -> 3"):
        validate_plan_json(plan, expected_address=ADDRESS)
