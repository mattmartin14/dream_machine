use pyo3::prelude::*;
use pyo3::types::PyModule;
use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;

// Pre-compile regex patterns once at startup - this is the KEY optimization!
static EMAIL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b").unwrap()
});
static URL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"https?://[^\s]+").unwrap()
});
static PHONE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b\d{3}-\d{3}-\d{4}\b|\b\(\d{3}\)\s*\d{3}-\d{4}\b").unwrap()
});
static DATE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b\d{1,2}/\d{1,2}/\d{4}\b|\b\d{4}-\d{2}-\d{2}\b").unwrap()
});
static NUMBER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b\d+\.?\d*\b").unwrap()
});
static WORD_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b\w+\b").unwrap()
});

/// A realistic string parsing UDF that analyzes text content
/// Now using pre-compiled static regex patterns for maximum performance
#[pyfunction]
fn parse_text_complexity(text: &str) -> PyResult<f64> {
    // Use the pre-compiled static regex patterns
    let email_count = EMAIL_REGEX.find_iter(text).count() as f64;
    let url_count = URL_REGEX.find_iter(text).count() as f64;
    let phone_count = PHONE_REGEX.find_iter(text).count() as f64;
    let date_count = DATE_REGEX.find_iter(text).count() as f64;
    let number_count = NUMBER_REGEX.find_iter(text).count() as f64;
    let word_count = WORD_REGEX.find_iter(text).count() as f64;
    
    // Calculate character-based metrics
    let char_count = text.len() as f64;
    let uppercase_count = text.chars().filter(|c| c.is_uppercase()).count() as f64;
    let digit_count = text.chars().filter(|c| c.is_numeric()).count() as f64;
    let special_char_count = text.chars().filter(|c| !c.is_alphanumeric() && !c.is_whitespace()).count() as f64;
    
    // Calculate word frequency entropy
    let mut word_freq: HashMap<String, i32> = HashMap::new();
    for word_match in WORD_REGEX.find_iter(text) {
        let word = word_match.as_str().to_lowercase();
        *word_freq.entry(word).or_insert(0) += 1;
    }
    
    let entropy = if word_count > 0.0 {
        word_freq.values()
            .map(|&freq| {
                let p = freq as f64 / word_count;
                -p * p.log2()
            })
            .sum::<f64>()
    } else {
        0.0
    };
    
    // Compute complexity score with weighted components
    let pattern_score = email_count * 2.0 + url_count * 1.5 + phone_count * 1.8 + date_count * 1.2;
    let density_score = if char_count > 0.0 {
        (uppercase_count + digit_count + special_char_count) / char_count * 10.0
    } else {
        0.0
    };
    let length_score = (char_count / 100.0).ln().max(0.0);
    let diversity_score = entropy;
    let numeric_density = if char_count > 0.0 { number_count / word_count * 5.0 } else { 0.0 };
    
    // Final complexity score
    let complexity = pattern_score + density_score + length_score + diversity_score + numeric_density;
    
    Ok(complexity)
}

/// Batch processing function using static regex patterns
#[pyfunction]
fn parse_text_complexity_batch(texts: Vec<String>) -> PyResult<Vec<f64>> {
    let mut results = Vec::with_capacity(texts.len());
    
    for text in &texts {
        // Use the pre-compiled static regex patterns
        let email_count = EMAIL_REGEX.find_iter(text).count() as f64;
        let url_count = URL_REGEX.find_iter(text).count() as f64;
        let phone_count = PHONE_REGEX.find_iter(text).count() as f64;
        let date_count = DATE_REGEX.find_iter(text).count() as f64;
        let number_count = NUMBER_REGEX.find_iter(text).count() as f64;
        let word_count = WORD_REGEX.find_iter(text).count() as f64;
        
        let char_count = text.len() as f64;
        let uppercase_count = text.chars().filter(|c| c.is_uppercase()).count() as f64;
        let digit_count = text.chars().filter(|c| c.is_numeric()).count() as f64;
        let special_char_count = text.chars().filter(|c| !c.is_alphanumeric() && !c.is_whitespace()).count() as f64;
        
        let mut word_freq: HashMap<String, i32> = HashMap::new();
        for word_match in WORD_REGEX.find_iter(text) {
            let word = word_match.as_str().to_lowercase();
            *word_freq.entry(word).or_insert(0) += 1;
        }
        
        let entropy = if word_count > 0.0 {
            word_freq.values()
                .map(|&freq| {
                    let p = freq as f64 / word_count;
                    -p * p.log2()
                })
                .sum::<f64>()
        } else {
            0.0
        };
        
        let pattern_score = email_count * 2.0 + url_count * 1.5 + phone_count * 1.8 + date_count * 1.2;
        let density_score = if char_count > 0.0 {
            (uppercase_count + digit_count + special_char_count) / char_count * 10.0
        } else {
            0.0
        };
        let length_score = (char_count / 100.0).ln().max(0.0);
        let diversity_score = entropy;
        let numeric_density = if char_count > 0.0 { number_count / word_count * 5.0 } else { 0.0 };
        
        let complexity = pattern_score + density_score + length_score + diversity_score + numeric_density;
        results.push(complexity);
    }
    
    Ok(results)
}

/// A Python module implemented in Rust.
#[pymodule]
fn string_parser_udf(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_text_complexity, m)?)?;
    m.add_function(wrap_pyfunction!(parse_text_complexity_batch, m)?)?;
    Ok(())
}
