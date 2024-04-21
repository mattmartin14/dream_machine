use std::time::Instant;

fn main() {
    let mut z: usize = 0;

    let start = Instant::now(); 

    for i in 0..=1_000_000_000 {
        z += i;
    }

    let elapsed = start.elapsed();

    println!("accumulated value is {}", z);
    println!("Time taken: {} milliseconds", elapsed.as_millis());
}