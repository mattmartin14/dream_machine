# Module 1 Basics

This module introduces Python, Bash, and SQL foundations for an up-and-coming data engineer.

The stack for this module is intentionally simple and modern:

- `uv` for Python project and dependency management
- `bash` for script automation and CLI-first workflows
- `duckdb` for local SQL analytics and file-based workflows
- `boto3` for AWS S3 interactions

## Lessons

1. [Python Basics with uv](python_basics.md)
2. [Bash Basics for Data Engineering Workflows](bash_basics.md)
3. [DuckDB Basics: Python, CLI, SQL Files, and S3](duckdb_basics.md)
4. [AWS Authentication with MFA (Cross-Platform)](aws_auth_mfa.md)

Bash is a core skill for data engineers. In many day-to-day workflows, shell commands and small scripts can accomplish the work quickly without needing a Python wrapper or a UI.

## Outcome

By the end of this module, the learner should be able to:

1. Initialize and manage a Python project with `uv`
2. Run Bash scripts and use executable permissions
3. Export and use environment variables in CLI workflows
4. Add and pin dependencies like `duckdb`
5. Use DuckDB from Python and CLI
6. Write reusable SQL in `.sql` files
7. Generate and query CSV/Parquet data
8. Push and pull Parquet files with AWS S3
