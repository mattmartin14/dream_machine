use clap::Parser;
use env_logger::Env;
use log::{info, warn};
use std::path::PathBuf;
use std::time::Instant;

mod generator;

/// Duckland data generator: orders (header+detail) and inventory CSVs.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Output directory for generated CSV files (e.g., ~/test_data)
    #[arg(short = 'o', long, default_value = "~/test_data")] 
    out: String,

    /// Number of parallel files to produce for orders (header & detail)
    #[arg(short = 'f', long, default_value_t = 10)]
    files: usize,

    /// Target combined size in GB for orders (header+detail across all files)
    #[arg(long = "orders-size-gb", default_value_t = 20.0)]
    orders_size_gb: f64,

    /// Target size in GB for inventory (single file)
    #[arg(long = "inventory-size-gb", default_value_t = 1.0)]
    inventory_size_gb: f64,

    /// Optional random seed for reproducibility
    #[arg(long)]
    seed: Option<u64>,
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(stripped);
        }
    }
    PathBuf::from(path)
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();
    let t0 = Instant::now();

    let out_dir = expand_tilde(&args.out);
    std::fs::create_dir_all(&out_dir)?;

    let seed = args.seed.unwrap_or_else(|| {
        let s = fastrand::u64(..);
        warn!("No seed provided; using random seed: {}", s);
        s
    });

    let orders_total_bytes = (args.orders_size_gb * 1_000_000_000f64) as u64;
    let inventory_bytes = (args.inventory_size_gb * 1_000_000_000f64) as u64;

    info!(
        "Starting generation → orders: {:.2} GB across {} files, inventory: {:.2} GB",
        args.orders_size_gb,
        args.files,
        args.inventory_size_gb
    );

    // Inventory first (single file)
    let inventory_skus = generator::generate_inventory_csv(&out_dir, "inventory", inventory_bytes, seed)?;

    // Orders (header + detail) split across N files and generated in parallel
    generator::generate_orders_csv_parallel(&out_dir, "orders", orders_total_bytes, args.files, seed, &inventory_skus)?;

    let elapsed = t0.elapsed();
    info!(
        "All datasets generated in {:?} ({}s)",
        out_dir,
        elapsed.as_secs_f64()
    );
    Ok(())
}
