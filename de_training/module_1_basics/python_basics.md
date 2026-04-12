# Python Basics with uv

## Goal
This lesson teaches beginner Python project setup and dependency management with uv. The key idea is simple: fewer setup steps means more time learning Python and data engineering.

## Why uv
Traditional setup usually looks like this:

1. Create a venv with `python -m venv .venv`
2. Activate it differently per shell/OS
3. Install dependencies with pip
4. Hope interpreter and dependencies stay aligned over time

With uv, common tasks are faster and more explicit:

1. Initialize project and Python version in one command
2. Add/remove/pin dependencies with one command style
3. Keep dependency resolution and lock state consistent

## Create a New Project
If starting from scratch:

```bash
uv init --python 3.13
```

This initializes the project and sets your Python target to 3.13.

## Add Dependencies
Add DuckDB without a version pin:

```bash
uv add duckdb
```

When no version is specified, uv resolves to the latest compatible version at that time.

## Pin or Pivot Versions Quickly
Need a specific version for reproducibility, debugging, or compatibility checks?

```bash
uv add duckdb==1.4.0
```

This updates your dependency declaration to that exact version, making it easy to test and compare behavior across versions.

## Optional: Add boto3 for AWS Work

```bash
uv add boto3
```

## Quick Verification
Check dependency state:

```bash
uv tree
```

Run a tiny import test:

```bash
uv run python -c "import duckdb; print(duckdb.__version__)"
```

## Mentor Talking Points
- uv removes environment-management noise so beginners focus on Python and SQL fundamentals.
- Version pinning is not just a packaging trick; it is a debugging and reproducibility skill.
- Switching versions quickly helps teach how dependency changes can affect behavior.
