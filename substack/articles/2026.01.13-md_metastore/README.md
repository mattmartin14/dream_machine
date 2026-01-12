# Retail Order Dataset Generator

Generates synthetic retail order headers and details with DuckDB and writes them to Parquet in `data/`.

## Environment

This project uses **uv** for Python environment and package management.

- Install deps: `uv sync`
- Add a dep: `uv add <package>` (updates [pyproject.toml](pyproject.toml) and lock)
- Create/update lockfile: `uv lock` (generates `uv.lock`)
- Run code: always use `uv run ...` — avoid activating or calling `.venv` directly.

## Usage (uv)

Run with uv and call the single function:

```bash
uv run python -c "import gen_order_data as g; g.generate_orders(1000)"
```

Outputs:
- `data/order_headers.parquet`
- `data/order_details.parquet`

The detail rows correspond to header `order_id` for easy joins.

## Store Geography

Generate store geography tied to `store_id`:

```bash
uv run python -c "import store_geography as sg; sg.generate_store_geography(50)"
```

Output:
- `data/store_geography.parquet` (columns: `store_id`, `city`, `state`, `zip`, `store_name`)

Use `store_id` to join with `order_headers.parquet`.

### Join Example (DuckDB)

Verify state distribution across orders by joining headers to geography:

```bash
uv run python -c "import duckdb as ddb; print(ddb.query(\"SELECT s.state, COUNT(*) AS orders FROM read_parquet('data/order_headers.parquet') h JOIN read_parquet('data/store_geography.parquet') s USING(store_id) GROUP BY s.state ORDER BY orders DESC\").fetchall())"
```

## Conventions

- We do not maintain a `requirements.txt`; dependencies live in [pyproject.toml](pyproject.toml) and are locked via `uv.lock`.
- Prefer `uv run python ...` over manual virtualenv activation to keep commands consistent across machines.
- If you see instructions referencing `.venv/bin/python`, replace them with an equivalent `uv run` command.

## Notebooks (uv)

To use Jupyter notebooks (`.ipynb`) with this project and keep all dependencies tracked by uv:

- Install the kernel package:

	```bash
	uv add ipykernel
	```

- Optional: register a named kernel for VS Code/Jupyter (helpful when you have many envs):

	```bash
	uv run python -m ipykernel install --user --name md-metastore --display-name "MD Metastore (uv)"
	```

- If you plan to run notebooks outside VS Code, you can also add a server:

	```bash
	uv add jupyterlab
	# or
	uv add notebook
	```

Notes:
- VS Code can use the environment directly once `ipykernel` is installed; a separate `jupyterlab` server is not required inside VS Code.
- Add any libraries you import in notebooks (e.g., `pandas`, `pyarrow`, `matplotlib`) via `uv add <package>` so they’re tracked.
