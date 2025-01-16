use pyo3::prelude::*;


#[pyfunction]
fn hello_world(name: String) -> PyResult<String> {
    Ok(format!("Hello World; greetings {}!", name))
}

#[pymodule]
fn fast_writer(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello_world, m)?)?;
    Ok(())
}


