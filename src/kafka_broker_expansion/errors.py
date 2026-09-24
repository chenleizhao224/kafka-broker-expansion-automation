"""Domain exceptions exposed by the expansion workflow."""


class ExpansionError(RuntimeError):
    """Base error for an expansion that must stop safely."""


class ConfigurationError(ExpansionError):
    """The supplied configuration is invalid or outside the V1 scope."""


class HealthCheckError(ExpansionError):
    """A Kafka, Kubernetes, or MirrorMaker health check failed."""


class UnsafePlanError(ExpansionError):
    """Terraform proposed a change outside the allow-list."""


class CommandError(ExpansionError):
    """An external command failed."""
