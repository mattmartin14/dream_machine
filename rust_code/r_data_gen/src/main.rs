use fakeit::{name, address, currency, contact};
use chrono::{NaiveDate, Duration};
use rand::Rng;
use std::error::Error;
use std::env;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::time::Instant;
use rayon::prelude::*;

// 5 seconds to write 1M rows
// 52 seconds for 10M rows
const TOTAL_ROWS: usize = 10_000_000;
const TOTAL_FILES: usize = 10;
const BATCH_SIZE: usize = 10_000;

// Function to generate a random date
fn gen_rand_dt() -> String {
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).expect("invalid date");
    let end_date = NaiveDate::from_ymd_opt(2024, 1, 1).expect("invalid date");

    let mut rng = rand::thread_rng();
    let days = rng.gen_range(0..(end_date - start_date).num_days() + 1);
    let random_date = start_date + Duration::days(days);
    random_date.format("%Y-%m-%d").to_string()
}

// Function to generate CSV headers
fn gen_headers() -> String {
    format!("{},{},{},{},{},{},{},{},{},{},{},{}",
        "first_name", "last_name", "street", "city", "state", "state_abr", "zip_cd",
        "latitude", "longitude", "net_worth", "email_adrs", "ref_dt")
}

// Function to generate a single data line
fn gen_data_line() -> String {
    let first_name = name::first();
    let last_name = name::last(); 
    let street = address::street(); 
    let city = address::city(); 
    let state = address::state(); 
    let state_abr = address::state_abr(); 
    let zip_cd = address::zip(); 
    let latitude = address::latitude(); 
    let longitude = address::longitude(); 
    let net_worth = currency::price(5000 as f64, 500000 as f64);
    let email_adrs = contact::email();
    let ref_dt = gen_rand_dt();

    format!("{},{},{},{},{},{},{},{},{},{},{},{}", 
        first_name, last_name, street, city, state, state_abr, zip_cd,
        latitude, longitude, net_worth, email_adrs, ref_dt)
}

// Function to write data to CSV file
fn write_data_to_file(file_path: &str, start_row: usize, end_row: usize) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);

    let headers = gen_headers();
    writeln!(writer, "{}", headers)?;

    for _row_num in start_row..=end_row {
        let row = gen_data_line();
        writeln!(writer, "{}", row)?;
  
        if _row_num % BATCH_SIZE == 0 || _row_num == end_row {
            writer.flush()?;
        }

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
    println!("Total Processing Time: {} seconds", elapsed);

    Ok(())
}