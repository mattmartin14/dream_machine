# FinOps model

`scripts/cost_report.py` can query Cost Explorer for the previous complete
calendar month. Historical billed totals are actual account data; permissions
or billing-data delays may prevent retrieval.

Estimates multiply measured resource quantities by rates supplied in `.env`.
The model covers Fargate vCPU/memory, S3 storage and requests, S3 Tables
storage, Secrets Manager storage/API calls, CloudWatch log ingestion/storage and
metrics, ECR storage, NAT processing, and network transfer. Every absent rate
remains unknown and makes the corresponding aggregate estimate incomplete.

Optional Hardcharger price inputs support an idle monthly base plus active hourly
usage. Confirm the contract model with GizmoData; these values may not be
additive. The sponsored demo expense is shown as zero for the complimentary
access only, while normal customer pricing remains unknown until supplied.

Capture measured task duration, vCPU/memory reservation, row counts, Parquet
volume, storage retention, and request/transfer metrics after a real run. Do not
present AWS list-price estimates as billed costs or claim database savings
without comparable, measured workloads.
