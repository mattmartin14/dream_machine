# DuckLake Aurora demo

This creates one Aurora PostgreSQL Serverless writer, with 0-1 ACU and
auto-pause after five minutes without connections. It uses the default VPC and
default subnets in `us-east-1`; they must exist in at least two availability
zones and have public internet routing. PostgreSQL port 5432 is exposed only
to the specified IPv4 address. This is a demo, not a highly available
production configuration.

AWS charges for the database and related resources. Destroy them after the
demo. Terraform state contains the admin password in plaintext; keep it private
and do not commit it. The generated `~/.zshrc` credentials are also plaintext,
although Terraform reads them from environment variables.
Local state, environment files, certificates, and DuckDB/DuckLake catalog
files are ignored by Git; do not force-add them. Git ignore rules do not
protect files already tracked or prevent someone from copying secrets into
other files.

From the project directory, in a Bash or zsh session where the credentials are set
(run `source ~/.zshrc` if needed):

```sh
source scripts/deploy.sh
```

Source the script rather than executing it: only sourcing can export
`AURORA_HOST` to the current shell. It refreshes the public IPv4 address from
AWS, downloads the AWS RDS global CA bundle to `~/.aws/rds-global-bundle.pem`,
initializes Terraform, and applies the configuration without a confirmation
prompt. It also exports `AURORA_SSL_ROOT_CERT` to the bundle path. The public IP and credentials are passed to
Terraform through environment variables, not a local inputs file. Rerun
the deploy script if your IP changes. Aurora creation can take several minutes.
Only run the ETL after apply completes.
Both scripts use a restrictive umask for Terraform state files.

The global PEM bundle is suitable for Aurora in any commercial AWS Region.
After deployment, run DuckDB in the same shell:

```sh
duckdb -c ".read etl/ducklake_etl.sql"
```

Run the ETL scripts from the project directory. For example, the Iceberg
workflow uses `duckdb -c ".read etl/iceberg_etl.sql"` and the data generator
uses `duckdb -c ".read etl/gen_data.sql"`.

Connections can take time to resume after auto-pause. The first run creates a new catalog;
the existing local `aws_dl1.ducklake` catalog is not migrated.

When finished, from the project directory:

```sh
bash scripts/destroy.sh
```

The destroy script supplies placeholder database credentials and CIDR to
Terraform; it does not need the Aurora login, but it does need AWS credentials.
**It destroys resources without a confirmation prompt.** It does not unset
`AURORA_HOST` in the parent shell; run `unset AURORA_HOST` afterward.
The cluster skips its final snapshot on deletion, so the Aurora catalog is
lost. The S3 data files are not removed. Delete any retained backups or other
resources separately, and remove local state and shell credentials when no
longer needed.
