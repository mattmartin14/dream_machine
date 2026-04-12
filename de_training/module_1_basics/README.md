# Module 1 Basics

This module introduces Python and SQL foundations for an up-and-coming data engineer.

The stack for this module is intentionally simple and modern:

- `uv` for Python project and dependency management
- `duckdb` for local SQL analytics and file-based workflows
- `boto3` for AWS S3 interactions

## Lessons

1. [Python Basics with uv](python_basics.md)
2. [DuckDB Basics: Python, CLI, SQL Files, and S3](duckdb_basics.md)
3. [AWS Authentication with MFA (Cross-Platform)](aws_auth_mfa.md)

## Outcome

By the end of this module, the learner should be able to:

1. Initialize and manage a Python project with `uv`
2. Add and pin dependencies like `duckdb`
3. Use DuckDB from Python and CLI
4. Write reusable SQL in `.sql` files
5. Generate and query CSV/Parquet data
6. Push and pull Parquet files with AWS S3
