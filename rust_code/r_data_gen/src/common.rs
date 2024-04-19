
// data generators


use fakeit::{name, address, currency, contact};
use chrono::{NaiveDate, Duration};
use rand::{Rng, thread_rng};
use std::fs::File;
use std::io::{Write, BufWriter};
use std::error::Error;


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

pub fn write_data_to_file(file_path: &str, start_row: usize, end_row: usize, buffer_size: usize, batch_size: usize) -> Result<(), Box<dyn Error>> {
    
    let file = File::create(file_path)?;
    //let mut writer = BufWriter::new(file);

    let mut writer = BufWriter::with_capacity(buffer_size, file);

    let headers = gen_headers();
    writeln!(writer, "{}", headers)?;

    let row_cnt = end_row - start_row+1;

    for chunk_start in (1..row_cnt).step_by(batch_size) {

        let chunk_end = (chunk_start + batch_size-1).min(row_cnt);

        for _row_num in chunk_start..=chunk_end {

            let data = gen_data_line();
            writeln!(writer, "{}", data)?;
        }
        writer.flush()?;
    }

    Ok(())
}


pub fn format_with_commas(value: usize) -> String {
    let mut result = String::new();
    let value_str = value.to_string();

    for (i, c) in value_str.chars().rev().enumerate() {
        if i != 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }

    result.chars().rev().collect::<String>()
}

// Function to generate CSV headers
pub fn gen_headers() -> String {
    format!("{},{},{},{},{},{},{},{},{},{},{}",
        "first_name", "last_name", "city", "state", "state_abr", "zip_cd",
        "latitude", "longitude", "net_worth", "email_adrs", "ref_dt")
}

pub fn gen_data_line() -> String {
    let first_name = name::first();
    let last_name = name::last(); 
    let city = address::city(); 
    let state = address::state(); 
    let state_abr = address::state_abr(); 
    let zip_cd = get_rand_zip(); //address::zip(); -- very slow using the fakeit zip grabber
    let latitude = address::latitude(); 
    let longitude = address::longitude(); 
    let net_worth = currency::price(5000 as f64, 500000 as f64);
    let email_adrs = contact::email();
    let ref_dt = gen_rand_dt();

    format!("{},{},{},{},{},{},{},{},{},{},{}", 
        first_name, last_name, city, state, state_abr, zip_cd,
        latitude, longitude, net_worth, email_adrs, ref_dt)
}


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