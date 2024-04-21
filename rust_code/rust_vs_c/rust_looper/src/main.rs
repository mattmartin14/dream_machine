use std::time::Instant;
use rand::{Rng, thread_rng};

fn main() {
    det_looper();
    non_det_looper();
}

fn det_looper() {
    let mut z: i64 = 0;

    let start = Instant::now(); 

    for i in 0..=1_000_000_000 {
        z += i;
    }

    let elapsed = start.elapsed();

    println!("accumulated value is {}", z);
    println!("Time taken for deterministic loop: {} milliseconds", elapsed.as_millis());
}

fn non_det_looper() {
    let mut z: i64 = 0;
    let mut rng = thread_rng();

    let start = Instant::now(); 

    for i in 0..=1_000_000_000 {
        let random_value: u8 = rng.gen_range(1..=26);
        z += i as i64 + random_value as i64;
    }

    let elapsed = start.elapsed();

    println!("accumulated value is {}", z);
    println!("Time taken for non-deterministic loop: {} milliseconds", elapsed.as_millis());
}
