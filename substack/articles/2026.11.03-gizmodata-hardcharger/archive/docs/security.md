# Security model

- AWS identities are obtained from the AWS credential chain. Terraform contains
  no keys or passwords.
- Hardcharger connection values are written to Secrets Manager after the actual
  managed cluster response is parsed. The ECS task role may read only that
  secret; credentials are not added to task environment values.
- Hardcharger ADBC TLS defaults to verified TLS. Do not use the driver's local
  `tls_skip_verify` development option. Apply vendor-provided CA/mTLS parameters
  only after confirming the cluster's settings.
- The ECS task role can read the TPC-H raw prefix, read the connection secret,
  publish the project metrics, and acquire/release the project lock. It has no
  S3 write permission. The task execution role handles image pulls/logging.
- The EventBridge Scheduler role can run only the ETL task definition and pass
  only its two ECS roles.
- Bucket public access is blocked and S3/S3 Tables encryption-at-rest is enabled.
  No inbound ECS rules exist.

The SQL policies follow the official Hardcharger row-policy and masking guide.
Regional filtering uses the source data's numeric region key represented as a
string in the aggregate table because session attributes are strings in the
documented example. Masking uses the documented `CREATE MASKING POLICY` and
`ALTER TABLE ... SET MASKING POLICY ... EXEMPT ROLE` forms.

Hardcharger documents that a table subject to policies is read-only for affected
users. System `READ FILESYSTEM` is broader than an individual S3 path in the
examples available here; confirm finer-grained controls before using real
customer data. All example customer PII is synthetic TPC-H data.

Before enabling a schedule, run the gated integration tests against the
configured cluster and confirm role ownership, policy behavior, write denials,
and credential rotation.
