# Sponsored article notes

Update these notes only with verified deployment measurements. No AWS deployment
or Hardcharger integration was run while scaffolding this project.

## Architectural decisions

- Fargate orchestrates SQL; managed Hardcharger executes SQL and reads S3 data.
- Daily output is recomputed transactionally by processing date.
- Schedule starts disabled; a DynamoDB conditional lock prevents overlaps.
- S3 Tables is optional; Parquet federation is the explicit fallback.
- TPC-H is synthetic representative data, not comparative benchmark evidence.

## Measured results

| Measurement | Result | Method/date |
|---|---:|---|
| TPC-H scale factor | Not run | |
| Parquet volume / per-table rows | Not run | |
| Upload duration | Not run | |
| Backfill duration | Not run | |
| Scheduled ETL duration | Not run | |
| Output rows / validation | Not run | |
| CloudWatch source/output measurements | Not run | |

## Security and AWS observations

- Security positive/negative integration tests: Not run.
- S3 Tables catalog support by managed Hardcharger: Unverified; ask GizmoData.
- Managed Hardcharger S3 access identity: Awaiting vendor configuration.
- AWS actual billed costs: Not queried.
- Sponsored Hardcharger expense: Complimentary for this demonstration.
- Normal Hardcharger price: Unknown pending published/supplied pricing.
- AWS integration challenges and limitations: Record after deployment.

## Article claims to avoid

- Do not call this a comparative TPC-H benchmark.
- Do not describe sponsor access as generally free.
- Do not claim S3 Tables federation works until the actual service setup and query
  pass; report the Parquet fallback separately.
- Do not describe static TPC-H records as daily-arriving production data.
