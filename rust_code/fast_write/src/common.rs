
use std::fs::File;
use std::io::{Write, BufWriter};
use std::error::Error;


// pub fn write_data_to_file(file_path: &str, start_row: usize, end_row: usize, buffer_size: usize, batch_size: usize) -> Result<(), Box<dyn Error>> {
    
//     let file = File::create(file_path)?;
//     //let mut writer = BufWriter::new(file);

//     let mut writer = BufWriter::with_capacity(buffer_size, file);

//     let row_cnt = end_row - start_row+1;

//     for chunk_start in (1..row_cnt).step_by(batch_size) {

//         let chunk_end = (chunk_start + batch_size-1).min(row_cnt);

//         for row_num in chunk_start..=chunk_end {

//             writeln!(writer, "{}", row_num)?;
//         }
//         writer.flush()?;
//     }

//     Ok(())
// }

pub fn write_data_to_file(file_path: &str, start_row: usize, end_row: usize, buffer_size: usize, batch_size: usize) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::with_capacity(buffer_size, file);

    let mut row_num = start_row;
    while row_num <= end_row {
        let chunk_end = (row_num + batch_size - 1).min(end_row);
        for chunk_row_num in row_num..=chunk_end {
            writeln!(writer, "{}", chunk_row_num)?;
        }
        writer.flush()?;
        row_num = chunk_end + 1;
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