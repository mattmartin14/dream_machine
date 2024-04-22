use std::time::Instant;
use rand::{Rng, thread_rng};

fn main() {
    det_looper();
    non_det_looper();
}

fn det_looper() {
    let mut z: i64 = 0;

    let start = Instant::now(); 

    for i in 1..=1_000_000_000 {
        z += i;
    }

    let elapsed = start.elapsed();

    println!("accumulated value is {}", z);
    println!("Time taken for deterministic loop in rust: {} milliseconds", elapsed.as_millis());
}

fn non_det_looper() {
    let mut z: i64 = 0;
    let mut rng = thread_rng();

    let start = Instant::now(); 

    for i in 1..=1_000_000_000 {
        // interesting: If i pre-assign the random value as an i64, the loop takes nearly 3x as long to complete
        // its faster just to cast the random value later so the add to z works
        // probably because u8 is a shorter value in ram vs. i64
        let random_value: u8 = rng.gen_range(1..=100);
        z += i as i64 + random_value as i64;
    }

    let elapsed = start.elapsed();

    println!("accumulated value is {}", z);
    println!("Time taken for non-deterministic loop in rust: {} milliseconds", elapsed.as_millis());
}
