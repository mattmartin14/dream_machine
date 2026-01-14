# rust_csv_gen

A minimal Rust CLI to generate CSV files. The actual fileset schema will be added later.

## Quick Start

```bash
# Build
cargo build

# Generate Duckland datasets (defaults → orders 20GB across 10 files; inventory 1GB)
cargo run -- --out ~/test_data --orders-size-gb 20 --inventory-size-gb 1 --files 10

# Faster release build/run
cargo build --release
cargo run --release -- --out ~/test_data --orders-size-gb 20 --inventory-size-gb 1 --files 10

# Run tests
cargo test
```

## CLI Options

- `--out` (`-o`): Output directory (default: `~/test_data`)
- `--files` (`-f`): Number of parallel files for orders (default: `10`)
- `--orders-size-gb`: Combined size target for orders header+detail (default: `20`)
- `--inventory-size-gb`: Size target for inventory (default: `1`)
- `--seed`: Optional RNG seed for reproducibility

## Project Layout

- `src/main.rs`: CLI entrypoint
- `src/lib.rs`: Library crate exposing modules
- `src/generator.rs`: Inventory and orders generators (parallel capable)
- `tests/`: Integration tests

## Notes

- CSV lines are written directly via `writeln!` for performance (no `csv` crate).
- Targets are byte-based; actual files may slightly exceed the target because rows are discrete.

## Next Steps

- Extend schemas to match business requirements when provided.
- Add progress reporting and throttling for very large runs.
