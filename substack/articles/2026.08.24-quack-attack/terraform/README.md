# Quack EC2 POC

This Terraform root deploys one Ubuntu ARM64 EC2 instance, an Elastic IP, a CIDR-restricted security group, and a private S3 bucket for DuckLake Parquet data. The instance's EBS root volume stores the Quack `.duckdb` catalog and defaults to `delete_root_volume_on_termination = false` so a stop/start or reboot preserves it. The bootstrap installs DuckDB 1.5.5 and starts Quack under systemd.

The server is launched by systemd as:

```bash
sleep infinity | duckdb -init /opt/quack-server/quack_server.sql /opt/quack-server/quack_catalog.duckdb
```

The positional catalog file is intentional: Quack creates fresh server-side connections and serves the DuckDB process's primary database. `quack.service` starts on boot and restarts after a process failure.

## Deploy

1. Copy `terraform.tfvars.example` to `terraform.tfvars` and supply your VPC, a **public** subnet, laptop egress CIDR, globally unique bucket name, and token. Terraform creates an EC2 key pair from `~/.ssh/quack-poc.pub` by default.
2. From the repository root, run `./scripts/deploy.sh` and approve the Terraform plan.
3. Wait for cloud-init, then inspect the service with `ssh -i ~/.ssh/quack-poc ubuntu@$(terraform -chdir=terraform output -raw quack_public_ip) 'sudo systemctl status quack'`.

The instance is ARM64 (`t4g.small`) to keep the POC inexpensive. Change `instance_type` to `t3.small` and provide an x86_64 `ami_id` if your environment requires Intel/AMD; the bootstrap script is currently ARM64-specific.

## Connect From A Laptop

Quack on port 9494 is plain HTTP. This POC deliberately has no TLS proxy, so `scripts/start_ducklake.sh` opens an SSH tunnel from `localhost:9494` on the laptop to port 9494 on EC2. Quack automatically uses HTTP for that local URI while SSH encrypts the laptop-to-EC2 connection. A Caddy/nginx TLS proxy and a domain are the next step for direct public client connections.

Start a local in-memory DuckDB session with the remote DuckLake catalog active as `dl1`:

```bash
./scripts/start_ducklake.sh
```

The script runs the following shape of SQL, printing the exact values for copy/paste into another local DuckDB terminal:

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL ducklake; LOAD ducklake;
INSTALL quack; LOAD quack;

CREATE OR REPLACE SECRET quack_secret (TYPE quack, TOKEN '<quack token>');
CREATE OR REPLACE SECRET ducklake_s3 (TYPE s3, PROVIDER credential_chain);

ATTACH 'ducklake:quack:localhost:9494' AS dl1 (
  DATA_PATH 's3://<ducklake bucket>/'
);
USE dl1;
```

`DATA_PATH` is shared S3 storage rather than a local path. The default `credential_chain` resolves the local AWS CLI/SSO session; run `aws sso login` first when needed. `USE dl1` ensures unqualified `CREATE TABLE` and `INSERT` statements target the remote DuckLake catalog. Network requests can run concurrently, but DuckDB/DuckLake catalog writes serialize; that is expected.

## Verify Persistence And Concurrency

After writing a row, run `ssh -i ~/.ssh/quack-poc ubuntu@<ip> 'sudo systemctl restart quack'`, wait for the service to be active, reconnect with `scripts/start_ducklake.sh`, and repeat the `SELECT`. The catalog survives because it is the process's EBS-backed primary database.

For the concurrency demonstration, start ten local processes using the same shared S3 `DATA_PATH`. They may issue reads in parallel; stagger or retry concurrent writes because catalog writes serialize.

## Teardown

Run `./scripts/destroy.sh` from the repository root. This POC teardown explicitly deletes all DuckLake Parquet data from S3 before destroying the bucket, as well as the instance, EIP, IAM resources, and networking. The detached root EBS volume must be removed manually if `delete_root_volume_on_termination` stays false.