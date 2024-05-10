
use std::fs::File;
use std::io::{Write, BufWriter};
use std::error::Error;

// slower than writeln!
pub fn write_data_to_file_v3(file_path: &str, start_row: usize, end_row: usize, buffer_size: usize, batch_size: usize) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::with_capacity(buffer_size, file);

    let mut row_num = start_row;
    let mut chunk = Vec::with_capacity(batch_size * 12); // Assuming an average of 12 bytes per integer

    while row_num <= end_row {
        let chunk_end = (row_num + batch_size - 1).min(end_row);
        for chunk_row_num in row_num..=chunk_end {
            chunk.extend_from_slice(chunk_row_num.to_string().as_bytes());
            chunk.push(b'\n'); // Add a newline after each number
        }
        writer.write_all(&chunk)?;
        writer.flush()?;
        chunk.clear();
        row_num = chunk_end + 1;
    }

    Ok(())
}

//slower than writeln!
pub fn write_data_to_file_v2(file_path: &str, start_row: usize, end_row: usize, buffer_size: usize, batch_size: usize) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::with_capacity(buffer_size, file);

    let mut row_num = start_row;
    while row_num <= end_row {
        let chunk_end = (row_num + batch_size - 1).min(end_row);
        for chunk_row_num in row_num..=chunk_end {
            let num_str = chunk_row_num.to_string();
            writer.write(num_str.as_bytes())?;
            writer.write(b"\n")?;
        }
        writer.flush()?;
        row_num = chunk_end + 1;
    }

    Ok(())
}


pub fn write_data_to_file(file_path: &str, start_row: usize, end_row: usize, buffer_size: usize, batch_size: usize) -> Result<(), Box<dyn Error>> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::with_capacity(buffer_size, file);

    let mut row_num = start_row;


    // this is slower; batching the flushes is faster
    // for row in row_num..=end_row {
    //     writeln!(writer, "{}", row)?;
    // }


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