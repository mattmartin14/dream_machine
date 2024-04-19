
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

    Below are the results to generate 100M rows of data accross the 3 strategies:

    Rayon - 256 seconds
    Tokio - 512 seconds
    Single Thread - 748 seconds


*/

mod common;

use clap::{App, Arg};
use std::error::Error;
use std::env;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::time::Instant;
use rayon::prelude::*;

//const TOTAL_ROWS: usize = 10_000;
const TOTAL_FILES: usize = 10;
const BATCH_SIZE: usize = 10_000;
const BUFFER_SIZE: usize = 1024 * 1024 * 10; //1 mb


// Function to write data to CSV file
fn write_data_to_file(file_path: &str, start_row: usize, end_row: usize) -> Result<(), Box<dyn Error>> {
    
    let file = File::create(file_path)?;
    //let mut writer = BufWriter::new(file);

    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);

    let headers = common::gen_headers();
    writeln!(writer, "{}", headers)?;

    let row_cnt = end_row - start_row+1;

    for chunk_start in (1..row_cnt).step_by(BATCH_SIZE) {

        let chunk_end = (chunk_start + BATCH_SIZE-1).min(row_cnt);

        for _row_num in chunk_start..=chunk_end {

            let data = common::gen_data_line();
            writeln!(writer, "{}", data)?;
        }
        writer.flush()?;
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let start_ts = Instant::now();
    let home_dir = env::var("HOME")?;
    //let row_cnt = TOTAL_ROWS;
    let files = TOTAL_FILES;


    // using clap to handle parameter passing
    let matches = App::new("Data Generator")
        .version("1.0")
        .author("Matt Martin")
        .about("Generates a fake dataset using Rust")
        .arg(
            Arg::with_name("rows")
                .short('r')
                .long("rows")
                .value_name("ROWS")
                .help("Sets the number of rows to generate")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let row_cnt: usize = matches
        .value_of("rows")
        .expect("Number of rows not provided")
        .parse()
        .expect("Invalid number of rows");


    // parallel process the data
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
    println!("Total Processing Time using Rayon for {} rows sharded to {} files: {} seconds", common::format_with_commas(row_cnt), files, elapsed);

    Ok(())
}