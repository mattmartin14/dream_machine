use fakeit::{name, address, currency, contact};
use chrono::{NaiveDate, Duration};
use rand::Rng;
use std::error::Error;
use std::env;
use std::fs::File;
use std::io::{Write, BufWriter};
use std::time::Instant;
use rayon::prelude::*;
use rand::{thread_rng};

// 5 seconds to write 1M rows
// 52 seconds for 10M rows
// 48 seconds with batch size 10k, buffer 10 mb
// changed the zip code to my own function; writes 10M in 25 seconds now
    // not using the street address either; it slows it down a lot
// writes 10 files, 100M rows in about 4 minutes
// 20 files for 100M rows completed in 236 seconds

// this is the rayon version; might want to try async-std from rust

const TOTAL_ROWS: usize = 100_000_000;
const TOTAL_FILES: usize = 20;
const BATCH_SIZE: usize = 10_000;
const BUFFER_SIZE: usize = 1024 * 1024 * 10; //1 mb
//const BUFFER_SIZE: usize = 4096;


// Function to generate a random date
fn gen_rand_dt() -> String {
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).expect("invalid date");
    let end_date = NaiveDate::from_ymd_opt(2024, 1, 1).expect("invalid date");

    let mut rng = rand::thread_rng();
    let days = rng.gen_range(0..(end_date - start_date).num_days() + 1);
    let random_date = start_date + Duration::days(days);
    random_date.format("%Y-%m-%d").to_string()
}

fn get_rand_zip() -> String {
    let mut rng = thread_rng();
    let zip_code: u32 = rng.gen_range(0..100000);
    format!("{:05}", zip_code) 
}

// Function to generate CSV headers
fn gen_headers() -> String {
    format!("{},{},{},{},{},{},{},{},{},{},{},{}",
        "first_name", "last_name", "street", "city", "state", "state_abr", "zip_cd",
        "latitude", "longitude", "net_worth", "email_adrs", "ref_dt")
}

// address streat, zip , and contact email add a lot of time
// Function to generate a single data line

// pre allocating a buffer appears to be even slower
// fn gen_data_line(buffer: &mut String) {
//     buffer.clear();  // Clear the buffer but keep its allocated memory

//     // Append each field to the buffer
//     buffer.push_str(&name::first());
//     buffer.push(',');
//     buffer.push_str(&name::last());
//     buffer.push_str(",,");  // Skip street, which is empty
//     buffer.push_str(&address::city());
//     buffer.push(',');
//     buffer.push_str(&address::state());
//     buffer.push(',');
//     buffer.push_str(&address::state_abr());
//     buffer.push(',');
//     buffer.push_str(&get_rand_zip());
//     buffer.push(',');
//     buffer.push_str(&address::latitude().to_string());
//     buffer.push(',');
//     buffer.push_str(&address::longitude().to_string());
//     buffer.push(',');
//     buffer.push_str(&currency::price(5000.0, 500000.0).to_string());
//     buffer.push(',');
//     buffer.push_str(&contact::email());
//     buffer.push(',');
//     buffer.push_str(&gen_rand_dt());
//     buffer.push('\n');  // Ensure new line for CSV format
// }

fn gen_data_line() -> String {
    let first_name = name::first();
    let last_name = name::last(); 
    let street = ""; //address::street(); -- very slow...might need to ditch
    let city = address::city(); 
    let state = address::state(); 
    let state_abr = address::state_abr(); 
    let zip_cd = get_rand_zip(); //address::zip(); -- very slow using the fakeit zip grabber
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
    //let mut writer = BufWriter::new(file);

    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
    //let mut data_line = String::with_capacity(512);  // Allocate once for likely max size of one line

    let headers = gen_headers();
    writeln!(writer, "{}", headers)?;

    let row_cnt = end_row - start_row;

    for chunk_start in (1..row_cnt).step_by(BATCH_SIZE) {

        let chunk_end = (chunk_start + BATCH_SIZE-1).min(row_cnt);

        for row_num in chunk_start..=chunk_end {

            //gen_data_line(&mut data_line);  // Generate line directly into the buffer
            //writer.write_all(data_line.as_bytes())?;

            let data = gen_data_line();
            writeln!(writer, "{}", data)?;
        }
        writer.flush()?;
    }

    // for _row_num in start_row..=end_row {
        
    //     let row = gen_data_line();
    //     writeln!(writer, "{}", row)?;

    //     if _row_num % BATCH_SIZE == 0 || _row_num == end_row {
    //         writer.flush()?;
    //     }

    // }


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