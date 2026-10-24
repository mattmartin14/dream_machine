.PHONY: install test lint format terraform-fmt terraform-validate hardcharger-up hardcharger-down

# Creates a billable managed Hardcharger cluster. Needs hardcharger/cluster-config.local.env,
# GIZMODATA_API_KEY and HC_ADMIN_PASSWORD in the environment, and a deployed Terraform stack
# (the connection secret is written to Secrets Manager).
hardcharger-up:
	bash hardcharger/provision.sh

# Terminates the cluster via the GizmoData API; needs GIZMODATA_API_KEY in the environment.
hardcharger-down:
	bash hardcharger/destroy.sh --confirm

install:
	uv sync --extra dev

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .

terraform-fmt:
	terraform -chdir=terraform fmt -check -recursive

terraform-validate:
	terraform -chdir=terraform init -backend=false
	terraform -chdir=terraform validate
