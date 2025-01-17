use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::env;
use std::time::Instant;
use num_format::{Locale, ToFormattedString};
use std::io::{BufWriter, Write};

pub fn write_alot() -> Result<(), Box<dyn Error>> {
    
    let start_ts = Instant::now();
    let buffer_size = 1024 * 1024 * 10; //10 mb

    let home_dir = env::var("HOME")?;
    let folder_path = home_dir.to_string() + "/test_dummy_data/write_benchmark";

    //println!("Home dir is: {:?}",folder_path);

    let file_path = Path::new(&folder_path).join("rust_generated.csv");
    let file = File::create(&file_path)?;

    let mut writer = BufWriter::with_capacity(buffer_size, file);
    //let mut writer = BufWriter::new(file);

    let row_cnt = 100_000_000;
    let batch_size = 10_000;
    
    for chunk_start in (1..row_cnt).step_by(batch_size) {

        let chunk_end = (chunk_start + batch_size-1).min(row_cnt);

        for row_num in chunk_start..=chunk_end {
            writeln!(writer, "{}", row_num)?;
        }
        writer.flush()?;
    }

    let elapsed = start_ts.elapsed().as_secs();

    let fmt_row_cnt = row_cnt.to_formatted_string(&Locale::en);

    println!("Rust Benchmark: CSV file written with {} rows. Total Processing Time {} seconds", fmt_row_cnt, elapsed);

    Ok(())

}