use anyhow::Result;
use fastrand::Rng;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

/// Writer wrapper that counts bytes written to reach target sizes precisely.
struct CountingWriter<W: Write> {
    inner: W,
    pub bytes: u64,
}

impl<W: Write> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, bytes: 0 }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub fn generate_inventory_csv(out_dir: &Path, base_filename: &str, target_bytes: u64, seed: u64) -> Result<Vec<u32>> {
    let file_path = out_dir.join(format!("{}{}.csv", base_filename, ""));
    let file = File::create(&file_path)?;
    let mut writer = CountingWriter::new(BufWriter::with_capacity(8 * 1024 * 1024, file));

    let mut rng = Rng::with_seed(seed);
    let categories = [
        "Decoys",
        "Apparel",
        "Camping",
        "Fishing",
        "Hunting",
        "Boating",
        "Optics",
        "Footwear",
    ];

    // header
    writeln!(writer, "sku,name,category,price_cents,stock_qty,location,updated_at")?;
    let now_str = OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_default();
    let mut skus: Vec<u32> = Vec::with_capacity(1_000_000);
    let mut sku_counter: u32 = 1;
    while writer.bytes < target_bytes {
        let category = categories[rng.usize(..categories.len())];
        let location = format!("Aisle-{}-Bin-{}", rng.u32(1..50), rng.u32(1..20));
        let name_id = rng.u32(..);
        writeln!(
            writer,
            "DUCK-{:08},Duckland Item {},{},{},{},{},{}",
            sku_counter,
            name_id,
            category,
            rng.u32(500..50_000),
            rng.u32(..10_000),
            location,
            now_str
        )?;
        skus.push(sku_counter);
        sku_counter = sku_counter.wrapping_add(1);
    }

    writer.flush()?;
    Ok(skus)
}

fn write_orders_pair(
    out_dir: &Path,
    base_filename: &str,
    file_index: usize,
    header_target_bytes: u64,
    detail_target_bytes: u64,
    seed: u64,
    id_offset: u64,
    inventory_skus: &[u32],
) -> Result<()> {
    let header_path = out_dir.join(format!("{}_header_{:02}.csv", base_filename, file_index));
    let detail_path = out_dir.join(format!("{}_detail_{:02}.csv", base_filename, file_index));

    let header_file = File::create(&header_path)?;
    let detail_file = File::create(&detail_path)?;

    let mut header_wtr = CountingWriter::new(BufWriter::with_capacity(8 * 1024 * 1024, header_file));
    let mut detail_wtr = CountingWriter::new(BufWriter::with_capacity(8 * 1024 * 1024, detail_file));

    let mut rng = Rng::with_seed(seed ^ (file_index as u64));
    let statuses = ["NEW", "PICK", "SHIP", "CLOSE", "CANCEL"];
    let channels = ["Store", "Online", "Mobile", "Marketplace"];
    let payments = ["Card", "Cash", "Gift", "ApplePay", "PayPal"];
    // categories removed; not used in order generation

    // headers
    writeln!(
        header_wtr,
        "order_id,customer_id,order_date,status,store_id,channel,payment_method,ship_zip,total_cents"
    )?;
    writeln!(
        detail_wtr,
        "order_id,line_id,sku,qty,unit_price_cents,line_total_cents"
    )?;

    let mut next_order_id = id_offset;
    let now_str = OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_default();
    while header_wtr.bytes < header_target_bytes || detail_wtr.bytes < detail_target_bytes {
        // Order header
        let order_id = next_order_id;
        next_order_id += 1;
        let customer_id = rng.u64(1..10_000_000);
        let order_date = &now_str;
        let status = statuses[rng.usize(..statuses.len())];
        let store_id = rng.u32(1..5000);
        let channel = channels[rng.usize(..channels.len())];
        let payment_method = payments[rng.usize(..payments.len())];
        let ship_zip_val = rng.u32(0..99_999);

        // Detail lines per order
        let lines = rng.u32(1..=10);
        let mut total_cents: u64 = 0;
        for line_id in 1..=lines {
            let qty = rng.u32(1..=5);
            let unit_price_cents = rng.u32(500..50_000);
            let line_total_cents = unit_price_cents as u64 * qty as u64;
            total_cents += line_total_cents;

            let sku_idx = rng.usize(..inventory_skus.len());
            let sku_val = inventory_skus[sku_idx];
            writeln!(
                detail_wtr,
                "{order_id},{line_id},DUCK-{sku:08},{qty},{unit_price_cents},{line_total_cents}",
                sku = sku_val
            )?;
        }

        writeln!(
            header_wtr,
            "{order_id},{customer_id},{order_date},{status},{store_id},{channel},{payment_method},{ship_zip:05},{total_cents}",
            ship_zip = ship_zip_val
        )?;

        // Stop when both targets reached
        if header_wtr.bytes >= header_target_bytes && detail_wtr.bytes >= detail_target_bytes {
            break;
        }
    }

    header_wtr.flush()?;
    detail_wtr.flush()?;
    Ok(())
}

pub fn generate_orders_csv_parallel(out_dir: &Path, base_filename: &str, total_target_bytes: u64, files: usize, seed: u64, inventory_skus: &[u32]) -> Result<()> {
    // Split total target across header and detail roughly equally
    let header_total = total_target_bytes / 2;
    let detail_total = total_target_bytes - header_total;

    let header_per_file = header_total / files as u64;
    let detail_per_file = detail_total / files as u64;

    (0..files).into_par_iter().try_for_each(|i| {
        // Give each file a disjoint order_id range offset to avoid collisions
        let id_offset = (i as u64) * 1_000_000_000; // large gap per file
        write_orders_pair(out_dir, base_filename, i, header_per_file, detail_per_file, seed, id_offset, inventory_skus)
    })?;

    Ok(())
}
