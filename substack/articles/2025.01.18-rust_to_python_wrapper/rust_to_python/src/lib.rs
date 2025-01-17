use pyo3::prelude::*;

mod fast_writer;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
fn hello_world(name: String) -> PyResult<String> {
    Ok(format!("Hello World; greetings {}!", name))
}

#[pyfunction]
fn write_fast() -> PyResult<String> {
    fast_writer::write_alot();
    Ok(format!("Success"))
}


/// A Python module implemented in Rust.
#[pymodule]
fn rust_to_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(hello_world, m)?)?;
    m.add_function(wrap_pyfunction!(write_fast, m)?)?;
    Ok(())
}
