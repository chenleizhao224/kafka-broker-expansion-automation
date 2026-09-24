.PHONY: install lint typecheck test check terraform-fmt terraform-validate run

install:
	python -m pip install -e '.[dev]'

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy src

test:
	pytest

terraform-fmt:
	terraform -chdir=terraform fmt -check -recursive

terraform-validate:
	terraform -chdir=terraform init -backend=false -input=false
	terraform -chdir=terraform validate

check: lint test terraform-fmt terraform-validate

run:
	kafka-expand --config config/cluster.yaml

