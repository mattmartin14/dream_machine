# Bash Basics for Data Engineering Workflows

## Goal
This lesson introduces Bash fundamentals that data engineers use every day: running scripts, exporting environment variables, and substituting runtime values into SQL workflows.

The key point is practical: you do not always need Python or a UI for useful data engineering work. Bash is often enough for orchestration and quick automation.

## Why Bash Matters for Data Engineers
Bash is a strong default when you want to:

1. Glue together command-line tools quickly
2. Run repeatable operational tasks without writing full Python programs
3. Parameterize scripts with environment variables for safer, reusable workflows

In this module, Bash sits between Python and DuckDB because it helps bridge script automation and SQL-first CLI work.

## Part 1: Run a Script (Two Common Methods)
Create a simple script file called `hello.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

name="${1:-data-engineer}"
echo "Hello, ${name}!"
```

### Method A: Invoke with Bash directly

```bash
bash hello.sh
bash hello.sh matt
```

This does not require executable permissions because you are explicitly launching the file with `bash`.

### Method B: Make it executable and run it directly

```bash
chmod +x hello.sh
./hello.sh
./hello.sh matt
```

This uses the shebang (`#!/usr/bin/env bash`) and runs the script as an executable program.

## Part 2: Export Environment Variables Before Running Commands
Environment variables are a standard way to parameterize scripts and avoid hardcoding values.

Set variables in your current shell session:

```bash
export AWS_BUCKET="my-training-bucket"
export ENVIRONMENT="dev"
```

Verify values:

```bash
echo "$AWS_BUCKET"
echo "$ENVIRONMENT"
```

Use exported variables when running commands:

```bash
duckdb -c "SELECT '$ENVIRONMENT' AS env_name;"
```

You can also set a variable inline for a single command (does not persist afterward):

```bash
AWS_BUCKET="my-training-bucket" duckdb -c "SELECT '$AWS_BUCKET' AS bucket_name;"
```

## Part 3: Runtime Variable Substitution into SQL
A common pattern is to keep SQL in files and inject values at runtime.

Create `query_template.sql`:

```sql
SELECT '${AWS_BUCKET}' AS bucket_name;
```

Then substitute with your environment variable and execute:

```bash
export AWS_BUCKET="my-training-bucket"
envsubst < query_template.sql | duckdb
```

How this works:

1. `envsubst` reads text from stdin
2. It replaces `${AWS_BUCKET}` with the current environment value
3. The resulting SQL is piped into DuckDB

This keeps SQL reusable while still allowing runtime parameterization.

## Repo-Aligned Example: DuckDB Script Flow
This repo already includes a script-style DuckDB SQL flow in `scripts/duckdb_aws.sql`.

Run it after setting your bucket:

```bash
export AWS_BUCKET="your-bucket-name"
duckdb -c ".read ./scripts/duckdb_aws.sql"
```

That file demonstrates two useful substitution patterns:

1. Shell variable usage in `.shell` commands (`$AWS_BUCKET`)
2. SQL-level environment reads with `getenv('AWS_BUCKET')`

## Practical Notes
- Quote variable expansions as `"$VAR"` in shell commands to avoid word-splitting issues.
- Prefer environment variables over hardcoded paths and credentials.
- For simple orchestration and parameterized runs, Bash can be faster to write and easier to run than a full Python wrapper.

## Mentor Talking Points
- Bash is not just a setup tool; it is a production-grade glue language for data workflows.
- Script execution mode (`bash script.sh` vs `./script.sh`) is a foundational operational skill.
- Environment-variable driven workflows create reusable scripts across local/dev/prod contexts.
- SQL templates plus runtime substitution are a practical bridge from shell automation into analytics workflows.
