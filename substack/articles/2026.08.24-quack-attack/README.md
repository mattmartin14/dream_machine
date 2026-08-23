# Quack EC2 POC

The `terraform/` module deploys one Ubuntu ARM64 EC2 instance, an Elastic IP, a CIDR-restricted security group, and a private S3 bucket for DuckLake Parquet data. The instance's EBS root volume stores the Quack `.duckdb` catalog and defaults to `delete_root_volume_on_termination = false` so a stop/start or reboot preserves it. The bootstrap installs DuckDB 1.5.5 and starts Quack under systemd.

The server is launched by systemd as:

```bash
sleep infinity | duckdb -init /opt/quack-server/quack_server.sql /opt/quack-server/quack_catalog.duckdb
```

The positional catalog file is intentional: Quack creates fresh server-side connections and serves the DuckDB process's primary database. `quack.service` starts on boot and restarts after a process failure.

## Deploy

1. Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars` and supply your VPC, a **public** subnet, laptop egress CIDR, globally unique bucket name, and token. Terraform creates an EC2 key pair from `~/.ssh/quack-poc.pub` by default.
2. From the repository root, run `./scripts/deploy.sh` and approve the Terraform plan.
3. Wait for cloud-init, then inspect the service with `ssh -i ~/.ssh/quack-poc ubuntu@$(terraform -chdir=terraform output -raw quack_public_ip) 'sudo systemctl status quack'`.

The instance is ARM64 (`t4g.small`) to keep the POC inexpensive. Change `instance_type` to `t3.small` and provide an x86_64 `ami_id` if your environment requires Intel/AMD; the bootstrap script is currently ARM64-specific.

## Connect From A Laptop

Quack on port 9494 is plain HTTP. This POC deliberately has no TLS proxy, so `scripts/start_ducklake.sh` opens an SSH tunnel from `localhost:9494` on the laptop to port 9494 on EC2. Quack automatically uses HTTP for that local URI while SSH encrypts the laptop-to-EC2 connection. A Caddy/nginx TLS proxy and a domain are the next step for direct public client connections.

The EC2 server owns the complete DuckLake setup: its `dl1` metadata catalog lives on EBS, and its Parquet data path is in the provisioned S3 bucket. DuckDB on EC2 uses its instance profile for S3 credentials. Clients do not load DuckLake or `httpfs`, provide a `DATA_PATH`, or configure AWS credentials.

Start a local in-memory DuckDB session attached only to Quack:

```bash
./scripts/start_ducklake.sh
```

The script runs the following shape of SQL, printing the exact values for copy/paste into another local DuckDB terminal:

```sql
INSTALL quack; LOAD quack;

CREATE OR REPLACE SECRET quack_secret (TYPE quack, TOKEN '<quack token>');
ATTACH 'quack:localhost:9494' AS remote;
```

With DuckDB 1.5.5, execute server-owned DuckLake SQL through Quack's query macro:

```sql
SELECT * FROM remote.query('CREATE TABLE dl1.test (id INTEGER)');
SELECT * FROM remote.query('INSERT INTO dl1.test VALUES (1), (2)');
SELECT * FROM remote.query('SELECT * FROM dl1.test');
```

Network requests can run concurrently, but DuckDB/DuckLake catalog writes serialize; that is expected. DuckDB 2.0 is expected to replace `remote.query(...)` with `CONNECT`, allowing ordinary SQL to run on the server.

## Verify Persistence And Concurrency

After writing a row, run `ssh -i ~/.ssh/quack-poc ubuntu@<ip> 'sudo systemctl restart quack'`, wait for the service to be active, reconnect with `scripts/start_ducklake.sh`, and repeat the `SELECT`. The catalog survives because it is the process's EBS-backed primary database.

For the concurrency demonstration, start ten local processes using the same shared S3 `DATA_PATH`. They may issue reads in parallel; stagger or retry concurrent writes because catalog writes serialize.

## Teardown

Run `./scripts/destroy.sh` from the repository root. This POC teardown deletes all managed resources: DuckLake Parquet data and its S3 bucket, the EC2 instance and root EBS volume, EIP, IAM resources, and networking. There is no retention prompt; a destroy is destructive by design.