# Duckland CSV Generator (src overview)

This module implements fast CSV generation for the Duckland outdoors store datasets:
- Inventory dataset (single CSV)
- Orders dataset (header + detail CSVs, generated in parallel)

It favors speed by:
- Writing lines directly via `writeln!` (no `csv` crate)
- Using large `BufWriter` buffers
- Using `fastrand` for fast RNG
- Reusing a single RFC3339 timestamp per file

## Files
- `main.rs`: CLI entrypoint, argument parsing, timing, and orchestration
- `generator.rs`: Core generators (`generate_inventory_csv`, `generate_orders_csv_parallel`)
- `lib.rs`: Exposes the `generator` module to tests and binaries

## CLI Usage
Run from the project root (`rust-csv-gen`):

```bash
# Dev build
cargo run -- --out ~/test_data --orders-size-gb 20 --inventory-size-gb 1 --files 10

# Faster release build
cargo build --release
cargo run --release -- --out ~/test_data --orders-size-gb 20 --inventory-size-gb 1 --files 10
```

### Arguments
- `--out` (`-o`): Output directory (default: `~/test_data`)
- `--files` (`-f`): Parallel files for orders header+detail (default: `10`)
- `--orders-size-gb`: Combined size target in GB for orders (default: `20`)
- `--inventory-size-gb`: Size target in GB for inventory (default: `1`)
- `--seed`: Optional RNG seed for reproducibility

## Output
- Inventory: `inventory.csv`
- Orders: `orders_header_00.csv ... orders_header_N.csv` and `orders_detail_00.csv ... orders_detail_N.csv`

## Notes
- Targets are byte-based; file sizes may slightly exceed targets because rows are discrete.
- Disk throughput is the main limiter. NVMe/SSD benefits from higher `--files`.
- Total elapsed time is logged on completion.
