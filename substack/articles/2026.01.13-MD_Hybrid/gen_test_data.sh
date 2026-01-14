#!/usr/bin/env bash
set -euo pipefail

# Run the Rust generator from the subfolder, regardless of where this script is called from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/rust-csv-gen"

# Simple invocation with the requested defaults.
#cargo run --release -- --out ~/test_data --orders-size-gb 5 --inventory-size-gb 0.5 --files 10
cargo run --release -- --out ~/test_data --orders-size-gb 20 --inventory-size-gb 1 --files 10

