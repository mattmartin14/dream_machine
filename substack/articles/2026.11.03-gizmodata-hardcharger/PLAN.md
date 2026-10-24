# Implementation plan

## Scope and safety

Build a local, reproducible demonstration project in this article folder. No AWS
resources will be created or changed during this work. Deployment scripts require
an explicit invocation and never enable the recurring schedule automatically.
Secrets, cluster API details, and pricing are user-supplied; none are fabricated.

## Verified platform facts

- Hardcharger's official Python client is `adbc-driver-hardcharger` and uses its
  Warp SQL ADBC driver. The documented URI defaults to TLS/HTTPS on port 31337.
- Hardcharger documents DuckDB 2.0, DuckLake support, and SQL-based RBAC.
- The documented row-policy syntax uses `CREATE ROW POLICY ... USING (...)`;
  masking uses `CREATE MASKING POLICY ...` and `ALTER TABLE ... SET MASKING POLICY`.
- Hardcharger instrumentation exposes SQL tables and Prometheus metrics. Its
  documentation says production instrumentation should use PostgreSQL, not a
  DuckLake catalog.
- No management API endpoint, request/response schema, or customer provisioning
  curl command is available in the checked official documentation. Provisioning
  therefore uses explicit endpoint, method, payload, response jq selectors, and
  auth inputs supplied locally.
- The AWS provider documents `aws_s3tables_table_bucket`,
  `aws_s3tables_namespace`, and `aws_s3tables_table`. This alone does not prove
  that a managed Hardcharger cluster can authenticate to S3 Tables' REST catalog.
- The supplied GizmoData cluster curl targets its `us-east-2` managed-service
  region. AWS infrastructure defaults to `us-east-2` for regional alignment,
  but remains independently configurable.

## Phases

1. **Scaffolding and infrastructure** — repository layout, Terraform, local
   provisioning adapter, deploy/destroy safety gates.
2. **TPC-H data** — reproducible DuckDB generation, manifests, and repeatable S3
   uploads.
3. **Connection and ETL** — typed configuration, Secrets Manager retrieval,
   ADBC connection, DuckLake setup interface, date-windowed and idempotent SQL.
4. **Scheduling and observability** — disabled EventBridge Scheduler schedule,
   CloudWatch logs/metrics/alarms, task-level JSON logging.
5. **Federation** — optional S3 Tables schema plus a separately invoked,
   fail-closed integration check; no unsupported catalog assumptions.
6. **Security** — documented Hardcharger RBAC/policy SQL grounded in official
   syntax and isolated positive/negative integration checks.
7. **FinOps, tests, and docs** — actual Cost Explorer data when permitted,
   configurable rate-based estimates, tests, runbooks, and article notes.

## Inputs required before live use

- GizmoData's deletion request, response jq selectors, and readiness behavior.
  The supplied creation curl establishes the endpoint, method, Bearer API key
  auth, and request payload.
- Managed Hardcharger connection URI, user, and secret; any TLS CA/client
  certificate requirements.
- A supported mechanism for the managed service to read the private S3 objects
  (for example, a GizmoData AWS principal to grant or another documented
  integration). Fargate's IAM role does not confer access to a separate service.
- Hardcharger's actual DuckLake catalog/storage configuration and, if S3 Tables
  federation is to be enabled, its supported catalog/auth configuration.
- Published or supplied non-complimentary Hardcharger pricing and any desired
  AWS pricing inputs for estimates.

## Validation record

- Local unit tests, formatting, and Terraform static validation are run after
  implementation where the required tools are installed.
- AWS deployment, managed-service provisioning, integrations, and recurring
  execution remain unrun until the user supplies inputs and explicitly approves
  those actions.
