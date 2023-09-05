/*
    Author: Matt Martin
    Date: 2023-08-28
    Desc: Rust Write Speed Test Harness 1B rows    

    steps to create/deply rust project
    go to top level folder, in terminal do "cargo new [project_folder_name]" e.g. "cargo new my_folder"
    cd into the project_folder_name > there should be a folder called "src" with a "main.rs" file
    -- open that file, edit, save
    -- then in terminal "cargo run"
    -- to compile a binary, do "cargo build" ; this makes a binary in the target/debug folder
    -- to make a more optimized release build, do "cargo build --release"; this makes a binary in target/release folder
        -- before doing the benchmarks, deploy the release executable...it is usually way faster than the debug version

    -- to link other files, the functions have to be marked "pub"
    --  Then you add in the main.rs a section up top to import the module using "mod [name of other module without rs extension]"
    -- functions in other files don't need to be uppercase to be recognized

    some perf notes:
        // takes 42 seconds for the csv writer to write 100M rows
        // takes only 3 seconds for the bufio writer to write 100m rows
        // took 37 seconds to write 1B rows -- in debug mode
        // took 16 seconds in release mode to write 1B rows
        // the csv writer is slow - dont use it

*/

use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::env;
use std::time::Instant;
use num_format::{Locale, ToFormattedString};
use std::io::{BufWriter, Write};

fn main() -> Result<(), Box<dyn Error>> {
    
    let start_ts = Instant::now();
    let buffer_size = 1024 * 1024 * 10; //10 mb

    let home_dir = env::var("HOME")?;
    let folder_path = home_dir.to_string() + "/test_dummy_data/write_benchmark";

    //println!("Home dir is: {:?}",folder_path);

    let file_path = Path::new(&folder_path).join("rust_generated.csv");
    let file = File::create(&file_path)?;

    let mut writer = BufWriter::with_capacity(buffer_size, file);
    //let mut writer = BufWriter::new(file);

    let row_cnt = 1_000_000_000;
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