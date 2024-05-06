
/*
    Author: Matt Martin
    Date: 5/6/24
    Desc: Uses Rayon to generate 1B rows across multiple text files

*/

mod common;

use clap::{App, Arg};
use std::error::Error;
use std::env;
use std::time::Instant;
use rayon::prelude::*;

const TOTAL_FILES: usize = 20;
const BATCH_SIZE: usize = 10_000;
const BUFFER_SIZE: usize = 1024 * 1024 * 10; //1 mb


fn main() -> Result<(), Box<dyn Error>> {
    let start_ts = Instant::now();
    let home_dir = env::var("HOME")?;
    let files = TOTAL_FILES;


    // using clap to handle parameter passing
    let matches = App::new("Data Generator")
        .version("1.0")
        .author("Matt Martin")
        .about("Generates a series of text files based on row count passed in")
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


    // parallel process the data using rayon
    (0..files).into_par_iter().for_each(|i| {
        let file_path = format!("{}/test_dummy_data/rust/data{}.txt", home_dir, i);
        let start_row = i * (row_cnt / files);
        //println!("Start row is {}",start_row);
        let end_row = if i == files - 1 {
            row_cnt - 1
        } else {
            (i + 1) * (row_cnt / files) - 1
        };

        if let Err(err) = common::write_data_to_file(&file_path, start_row, end_row, BUFFER_SIZE, BATCH_SIZE) {
            eprintln!("Error writing to file {}: {}", file_path, err);
        }
    });

    let elapsed = start_ts.elapsed().as_secs_f64();
    println!("Total Processing Time using Rayon for {} rows sharded to {} files: {:.2} seconds", common::format_with_commas(row_cnt), files, elapsed);

    Ok(())
}