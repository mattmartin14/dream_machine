# Federation

The Terraform table bucket and Iceberg table are a provisioning baseline. The
customer-segment table has an Iceberg schema for `nationkey` and `segment`; it
starts empty. The provider resource does not populate rows.

Hardcharger's general DuckDB and DuckLake support does not on its own establish
that the managed service can authenticate to the S3 Tables REST catalog. Verify
the supported catalog/REST endpoint, AWS credential flow, network access, and
required server configuration with GizmoData before enabling that query.

If supported, populate the S3 Tables table using the documented AWS workflow
available in the account, configure the catalog on the managed server, then run
`sql/federation.sql` via `scripts/run_federation.py`.

If unsupported, use `scripts/generate_reference.py` and
`scripts/upload_reference.py`, then run
`sql/federation_parquet_fallback.sql`. The fallback joins the DuckLake daily
table to a small synthetic Parquet reference file directly from the service.
Both modes execute SQL in Hardcharger; result batches return to the client.
Neither path downloads the multi-gigabyte TPC-H dataset to Fargate.
