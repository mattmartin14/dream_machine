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
make deploy
```

Make requires `AURORA_ADMIN_USER` and `AURORA_ADMIN_PWD` to be exported in
the invoking shell, along with AWS credentials. Deployment refreshes the public
IPv4 address from AWS, downloads the AWS RDS global CA bundle to
`~/.aws/rds-global-bundle.pem`,
initializes Terraform, and applies the configuration without a confirmation
prompt. The public IP and credentials are passed to
Terraform through environment variables, not a local inputs file. Rerun
the deploy script if your IP changes. Aurora creation can take several minutes.
Only run the ETL after apply completes.
Both scripts use a restrictive umask for Terraform state files.

The global PEM bundle is suitable for Aurora in any commercial AWS Region.
After deployment, run the DuckLake ETL:

```sh
make etl
```

The ETL target reads the Aurora endpoint from Terraform state and uses the
downloaded CA bundle, or `AURORA_SSL_ROOT_CERT` if set. It requires the
exported Aurora and AWS credentials and `SLACK_WEBHOOK` used by the ETL.
It does not deploy infrastructure automatically. Make runs each target in a
child shell, so `make deploy` does not export the connection settings to your
current shell. Running the SQL directly without `AURORA_HOST` makes PostgreSQL
try a local socket instead of Aurora. To run DuckDB directly without
redeploying, set the connection settings in the current shell:

```sh
if AURORA_HOST=$(terraform -chdir=terraform output -raw aurora_host) && [ -n "$AURORA_HOST" ]; then
  export AURORA_HOST
  export AURORA_SSL_ROOT_CERT="${AURORA_SSL_ROOT_CERT:-$HOME/.aws/rds-global-bundle.pem}"
  duckdb -c ".read etl/ducklake_etl.sql"
else
  echo "Could not read the Aurora endpoint from Terraform." >&2
fi
```

Alternatively, source `scripts/deploy.sh` in the current Bash or zsh session
to apply the infrastructure and export both settings.

Run the ETL scripts from the project directory. For example, the Iceberg
workflow uses `duckdb -c ".read etl/iceberg_etl.sql"` and the data generator
uses `duckdb -c ".read etl/gen_data.sql"`.

Connections can take time to resume after auto-pause. The first run creates a new catalog;
the existing local `aws_dl1.ducklake` catalog is not migrated.

When finished, from the project directory:

```sh
make destroy
```

The destroy script supplies placeholder database credentials and CIDR to
Terraform; it does not need the Aurora login, but it does need AWS credentials.
**It destroys resources without a confirmation prompt.** It does not unset
`AURORA_HOST` in the parent shell; if you previously sourced the deploy script,
run `unset AURORA_HOST AURORA_SSL_ROOT_CERT` afterward.
The cluster skips its final snapshot on deletion, so the Aurora catalog is
lost. The S3 data files are not removed. Delete any retained backups or other
resources separately, and remove local state and shell credentials when no
longer needed.
