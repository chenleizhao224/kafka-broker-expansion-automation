"""Command-line entry point."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from pathlib import Path

from .config import load_config
from .errors import ExpansionError
from .kafka_checks import KafkaChecker
from .kubernetes_checks import KubernetesChecker
from .logging import configure_logging, log_event
from .terraform import TerraformRunner, ValidatedPlan
from .workflow import ExpansionWorkflow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafka-expand",
        description="Safely expand the configured Kafka StatefulSet from two brokers to three.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/cluster.example.yaml"),
        help="YAML configuration path (default: config/cluster.example.yaml)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply the validated plan without an interactive confirmation",
    )
    parser.add_argument("--verbose", action="store_true", help="Include wait-loop diagnostics")
    return parser


def _confirmation(auto_approve: bool) -> Callable[[ValidatedPlan], bool]:
    def approve(plan: ValidatedPlan) -> bool:
        if auto_approve:
            return True
        response = input(
            f"Apply the validated {plan.before_replicas} -> {plan.after_replicas} "
            "broker expansion? [y/N] "
        )
        return response.strip().lower() in {"y", "yes"}

    return approve


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logger = configure_logging(args.verbose)
    try:
        settings = load_config(args.config)
        workflow = ExpansionWorkflow(
            settings=settings,
            kafka=KafkaChecker(settings.kafka, settings.mirrormaker),
            kubernetes=KubernetesChecker(settings.kubernetes),
            terraform=TerraformRunner(settings.terraform),
            logger=logger,
        )
        workflow.run(_confirmation(args.yes))
    except (ExpansionError, KeyboardInterrupt) as exc:
        log_event(
            logger,
            logging.ERROR,
            "expansion_failed",
            "Kafka broker expansion stopped",
            reason=str(exc),
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
