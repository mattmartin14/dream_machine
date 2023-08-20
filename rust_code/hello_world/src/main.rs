/*
    steps for rust
    go to top level folder, in terminal do "cargo new project_folder_name"
    cd into the project_folder_name > there should be a folder called "src" with a "main.rs" file
    -- open that file, edit, save
    -- then in terminal "cargo run"
    -- to compile a binary, do "cargo build" ; this makes a binary in the target/debug folder
    -- to make a more optimized release build, do "cargo build --release"; this makes a binary in target/release folder

    -- to link other files, the functions have to be marked "pub"
    --  Then you add in the main.rs a section up top to import the module using "mod [name of other module without rs extension]"
    -- functions in other files don't need to be uppercase to be recognized

*/

extern crate csv;

mod test2;

use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::env;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello, world!");
    test();
    test2::test2p();

    //let f_path string = "~/test_dummy_data/rust/test.csv";
    let home_dir = env::var("HOME")?;
    let folder_path = home_dir.to_string() + "/test_dummy_data/rust";

    println!("Home dir is: {:?}",folder_path);
    //println!(home_dir);

    let file_path = Path::new(&folder_path).join("output.csv");
    
    //println!(file_path);
    //let file_path = Path::new("~/test_dummy_data/rust/test.csv");
    let file = File::create(&file_path)?;

    let mut csv_writer = csv::Writer::from_writer(file);

    // Write headers if needed
    csv_writer.write_record(&["Value"])?;

    // Write 1000 integers to the CSV file
    // slow at 100M; need to flush the writer every 10k rows i'm guessing



    for i in 1..=100000000 {
        csv_writer.write_record(&[i.to_string()])?;


    }

    // Flush and finish writing
    csv_writer.flush()?;

    println!("CSV file written successfully.");
    println!("CSV file written successfully to: {:?}", file_path);

    Ok(())

}

fn test() {
    println!("test test")
}
