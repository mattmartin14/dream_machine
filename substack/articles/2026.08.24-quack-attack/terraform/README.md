# Quack EC2 POC

This Terraform root deploys one Ubuntu ARM64 EC2 instance, an Elastic IP, a CIDR-restricted security group, and a private S3 bucket for DuckLake Parquet data. The instance's EBS root volume stores the Quack `.duckdb` catalog and defaults to `delete_root_volume_on_termination = false` so a stop/start or reboot preserves it.

The server is launched by systemd as:

```bash
sleep infinity | duckdb -init /opt/quack-server/quack_server.sql /opt/quack-server/quack_catalog.duckdb
```

The positional catalog file is intentional: Quack creates fresh server-side connections and serves the DuckDB process's primary database. `quack.service` starts on boot and restarts after a process failure.

## Deploy

1. Copy `terraform.tfvars.example` to `terraform.tfvars` and supply your VPC, a **public** subnet, laptop egress CIDR, globally unique bucket name, and token. Terraform creates an EC2 key pair from `~/.ssh/quack-poc.pub` by default.
2. Run `terraform init`, `terraform plan`, and `terraform apply`.
3. Wait for cloud-init, then inspect the service with `ssh ubuntu@$(terraform output -raw quack_public_ip) 'sudo systemctl status quack'`.

The instance is ARM64 (`t4g.small`) to keep the POC inexpensive. Change `instance_type` to `t3.small` and provide an x86_64 `ami_id` if your environment requires Intel/AMD; the bootstrap script is currently ARM64-specific.

## Connect From A Laptop

Quack on port 9494 is plain HTTP. This POC deliberately has no TLS proxy, so only use it from the restricted CIDRs and tell Quack to disable SSL validation. An nginx/Caddy reverse proxy and a real domain are the next step when TLS is needed.

Install/load `httpfs`, `ducklake`, and `quack` locally. Configure S3 credentials locally using your AWS profile or DuckDB secret, then attach with the Terraform outputs:

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL ducklake; LOAD ducklake;
INSTALL quack; LOAD quack;

CREATE OR REPLACE SECRET quack_secret (TYPE quack, TOKEN '<terraform.tfvars quack_token>');
CREATE OR REPLACE SECRET ducklake_s3 (
  TYPE s3,
  PROVIDER credential_chain,
  CHAIN 'env;config'
);

ATTACH 'ducklake:quack:<terraform output -raw quack_uri>' AS dl1 (
  DATA_PATH 's3://<terraform output -raw ducklake_bucket_name>/data/',
  DISABLE_SSL true
);

CREATE TABLE IF NOT EXISTS dl1.test (id INTEGER);
INSERT INTO dl1.test VALUES (1), (2);
SELECT * FROM dl1.test;
```

`DATA_PATH` is shared S3 storage rather than a local path. Client processes need their own valid AWS credentials to use it. Network requests can run concurrently, but DuckDB/DuckLake catalog writes serialize; that is expected.

## Verify Persistence And Concurrency

After writing a row, run `ssh ubuntu@<ip> 'sudo systemctl restart quack'`, wait for the service to be active, reconnect, and repeat the `SELECT`. The catalog survives because it is the process's EBS-backed primary database.

For the concurrency demonstration, start ten local processes using distinct DuckDB working databases and the same shared S3 `DATA_PATH`. They may issue reads in parallel; stagger or retry concurrent writes because catalog writes serialize.

## Teardown

`terraform destroy` removes the instance, EIP, IAM resources, and networking. By default it will fail rather than delete a non-empty S3 bucket. Set `force_destroy_ducklake_bucket = true` only when its data can be discarded. The detached root EBS volume must be removed manually if `delete_root_volume_on_termination` stays false.