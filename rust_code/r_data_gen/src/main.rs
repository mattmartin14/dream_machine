
/*
    Author: Matt Martin
    Date: 4/18/24
    Desc: Generates a fake dataset using Rust.

    I tried 3 approaches for generating the files fast:
        1) Tokio async
        2) Single Threaded
        3) Rayon


    Rayon was the fastest out of the 3; i'm finding that this process is very much CPU bound when I run it.

    Tokio (using async) was suprisingly slower, but its built for more I/O Bound tasks


*/

mod common;
use common::*;

use std::error::Error;
use std::env;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::time::Instant;
use rayon::prelude::*;
//use rand::{thread_rng};

// 5 seconds to write 1M rows
// 52 seconds for 10M rows
// 48 seconds with batch size 10k, buffer 10 mb
// changed the zip code to my own function; writes 10M in 25 seconds now
    // not using the street address either; it slows it down a lot
// writes 10 files, 100M rows in about 4 minutes
// 20 files for 100M rows completed in 236 seconds

// this is the rayon version; might want to try async-std from rust

const TOTAL_ROWS: usize = 100_000_000;
const TOTAL_FILES: usize = 10;
const BATCH_SIZE: usize = 10_000;
const BUFFER_SIZE: usize = 1024 * 1024 * 10; //1 mb


// Function to write data to CSV file
fn write_data_to_file(file_path: &str, start_row: usize, end_row: usize) -> Result<(), Box<dyn Error>> {
    
    let file = File::create(file_path)?;
    //let mut writer = BufWriter::new(file);

    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);

    let headers = gen_headers();
    writeln!(writer, "{}", headers)?;

    let row_cnt = end_row - start_row+1;

    for chunk_start in (1..row_cnt).step_by(BATCH_SIZE) {

        let chunk_end = (chunk_start + BATCH_SIZE-1).min(row_cnt);

        for row_num in chunk_start..=chunk_end {

            let data = gen_data_line();
            writeln!(writer, "{}", data)?;
        }
        writer.flush()?;
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let start_ts = Instant::now();
    let home_dir = env::var("HOME")?;
    let row_cnt = TOTAL_ROWS;
    let files = TOTAL_FILES;

    (0..files).into_par_iter().for_each(|i| {
        let file_path = format!("{}/test_dummy_data/rust/data{}.csv", home_dir, i);
        let start_row = i * row_cnt / files;
        let end_row = if i == files - 1 {
            row_cnt - 1
        } else {
            ((i + 1) * row_cnt) / files - 1
        };

        if let Err(err) = write_data_to_file(&file_path, start_row, end_row) {
            eprintln!("Error writing to file {}: {}", file_path, err);
        }
    });

    let elapsed = start_ts.elapsed().as_secs();
    println!("Total Processing Time using Rayon for {} rows sharded to {} files: {} seconds", row_cnt, files, elapsed);

    Ok(())
}