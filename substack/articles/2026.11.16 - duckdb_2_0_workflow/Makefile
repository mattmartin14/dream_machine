.PHONY: help deploy etl destroy

help:
	@printf '%s\n' 'make deploy  - create or update the Aurora resources' \
		'make etl     - run the DuckLake ETL against the deployed resources' \
		'make destroy - destroy the Aurora resources without confirmation'

deploy:
	bash -c 'source scripts/deploy.sh'

etl:
	@host="$$(terraform -chdir=terraform output -raw aurora_host)" || exit $$?; \
	if [ -z "$$host" ]; then \
		printf '%s\n' 'Terraform returned an empty Aurora endpoint.' >&2; \
		exit 1; \
	fi; \
	cert="$${AURORA_SSL_ROOT_CERT:-$$HOME/.aws/rds-global-bundle.pem}"; \
	if [ ! -r "$$cert" ]; then \
		printf 'Aurora CA bundle is not readable: %s\n' "$$cert" >&2; \
		exit 1; \
	fi; \
	AURORA_HOST="$$host" AURORA_SSL_ROOT_CERT="$$cert" \
		duckdb -c ".read etl/ducklake_etl.sql"

destroy:
	bash scripts/destroy.sh
