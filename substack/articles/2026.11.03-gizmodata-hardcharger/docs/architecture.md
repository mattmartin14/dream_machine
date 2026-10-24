# Architecture

```text
DuckDB TPC-H generator
        |
        v
private S3 raw bucket (Parquet + manifest)
        |
        | managed Hardcharger service-side S3 identity (must be confirmed)
        v
Hardcharger ADBC / Warp SQL <--- TLS ---> ECS Fargate ETL orchestrator
        |                                  | S3 manifest, Secrets Manager,
        v                                  | DynamoDB lock, CloudWatch logs/metrics
DuckLake tables                           |
        ^                                  ^
        |                                  |
optional S3 Tables Iceberg dataset     EventBridge Scheduler (disabled)
```

Fargate orchestrates bounded SQL calls; Hardcharger runs the joins/aggregations
and reads the Parquet paths from S3. Fargate does not materialize the full source
dataset. An optional S3 Tables query is a separate integration path. The
provider can provision the S3 Tables resources, but managed Hardcharger catalog
and authentication support needs vendor confirmation.

The default VPC has public subnets and outbound-only task security groups. A
public IP provides external TLS connectivity without a NAT Gateway. Existing
private subnets can be used when appropriate endpoints/NAT and outbound routes
are configured. No vendor private endpoint is assumed.

The central external dependency is managed-service access to private S3. An ECS
task role never authorizes a different service. A vendor IAM principal must be
identified and granted narrowly, or another documented S3 credential mechanism
must be configured.
