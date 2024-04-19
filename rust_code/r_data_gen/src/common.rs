
// data generators


use fakeit::{name, address, currency, contact};
use chrono::{NaiveDate, Duration};
use rand::{Rng, thread_rng};


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