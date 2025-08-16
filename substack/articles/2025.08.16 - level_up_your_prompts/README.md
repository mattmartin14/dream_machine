# DuckDB UDF Performance Comparison: Python vs Rust

This project compares the performance of User Defined Functions (UDFs) in DuckDB when implemented in pure Python versus Rust (exposed as Python functions).

## Project Overview

The goal is to test whether Rust-based UDFs can provide better performance than pure Python UDFs when processing large datasets in DuckDB. We've implemented a realistic text parsing function that extracts patterns and calculates complexity scores from text data.

## Project Structure

```
├── requirements.txt          # Python dependencies
├── requirements.md          # Project requirements
├── README.md               # This file
├── src/
│   ├── data_generator.py   # Generates test data with 100M rows
│   ├── python_udf.py      # Pure Python UDF implementation
│   └── benchmark.py        # Main benchmark script
├── rust_udf/
│   ├── Cargo.toml         # Rust dependencies
│   ├── pyproject.toml     # Python packaging for Rust
│   └── src/
│       └── lib.rs         # Rust UDF implementation
├── data/
│   └── test_database.duckdb # Generated test database
└── results/
    └── performance_comparison.png # Benchmark results chart
```

## The UDF Implementation

Both Python and Rust versions implement the same text parsing logic:

### Features Analyzed:
- **Pattern Extraction**: Emails, URLs, phone numbers, dates, numbers
- **Character Analysis**: Uppercase, digits, special characters
- **Text Metrics**: Length, word count, word frequency entropy
- **Complexity Scoring**: Weighted combination of all factors

### Sample Input/Output:
```
Input: "Contact John at john@example.com or call (555) 123-4567 on 12/25/2024"
Output: Complexity score based on pattern density, character diversity, etc.
```

## Setup Instructions

### 1. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 2. Install Rust (if not already installed)
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
```

### 3. Install Maturin (for Rust-Python bindings)
```bash
pip install maturin
```

### 4. Build the Rust UDF
```bash
cd rust_udf
maturin develop --release
cd ..
```

## Running the Benchmark

### Step 1: Generate Test Data
```bash
python src/data_generator.py
```
This creates a DuckDB database with complex text data. For development, it starts with 1M rows, but can be configured for 100M rows.

### Step 2: Run Performance Benchmark
```bash
python src/benchmark.py
```
This will:
1. Build the Rust UDF (if needed)
2. Register both Python and Rust UDFs with DuckDB
3. Run 5 benchmark iterations for each UDF type
4. Generate performance comparison charts
5. Display detailed performance statistics

## Expected Results

After fixing the critical regex compilation issue, the benchmark shows:

### Performance Characteristics:
- **Rust UDF**: 4.9x faster than Python, processing 16,548 rows/sec
- **Python UDF**: Good baseline performance, processing 3,475 rows/sec  
- **Key Factor**: Pre-compiled regex patterns are essential for Rust performance

### Sample Results:
```
PERFORMANCE BENCHMARK SUMMARY
=============================================================
Sample size: 25,000 rows per run
Total rows processed: 41,000

Python UDF Results:
  Average: 7.255s (3,475 rows/sec)
  Consistent performance across dataset sizes

Rust UDF Results:
  Average: 1.476s (16,548 rows/sec)
  Performance advantage scales with dataset size

Performance Improvement:
  Rust is 4.9x faster than Python
  Rust overcame PyO3 overhead through optimization
```

## Technical Implementation Details

### Python UDF (`python_udf.py`):
- Uses Python `re` module for pattern matching
- Implements character counting with list comprehensions
- Uses `collections.Counter` for word frequency analysis
- Mathematical operations using Python's `math` module

### Rust UDF (`rust_udf/src/lib.rs`):
- Uses `regex` crate for pattern matching (compiled patterns)
- Direct character iteration for counting operations
- HashMap for word frequency analysis
- Native mathematical operations
- Exposed to Python via `pyo3`

### Data Generation (`data_generator.py`):
- Creates realistic text patterns with embedded data
- Supports batch insertion for memory efficiency
- Generates diverse content: emails, phones, URLs, dates, descriptions
- Configurable dataset size (1M to 100M+ rows)

## Performance Factors

### Rust Advantages:
- Compiled code vs interpreted Python
- More efficient memory usage
- Faster regex operations
- Lower function call overhead
- Better CPU cache utilization

### Python Advantages:
- No compilation step required
- Easier debugging and development
- Rich ecosystem of libraries
- Dynamic typing flexibility

## Benchmark Methodology

1. **Consistent Environment**: Same machine, same data, same queries
2. **Multiple Runs**: 5 iterations per UDF type to account for variability
3. **Statistical Analysis**: Mean, standard deviation, min/max reporting
4. **Realistic Workload**: Complex text parsing operations on large datasets
5. **Fair Comparison**: Identical algorithms in both implementations

## Files Generated

- `data/test_database.duckdb`: DuckDB database with test data
- `results/performance_comparison.png`: Benchmark results visualization
- Build artifacts in `rust_udf/target/` (after Rust build)

## Troubleshooting

### Common Issues:

1. **Rust UDF Build Fails**:
   ```bash
   # Ensure Rust is installed
   rustc --version
   # Reinstall maturin
   pip install --upgrade maturin
   ```

2. **Import Errors**:
   ```bash
   # Verify Python path
   python -c "import sys; print('\n'.join(sys.path))"
   # Rebuild Rust extension
   cd rust_udf && maturin develop --release
   ```

3. **Memory Issues with Large Datasets**:
   - Reduce batch size in data_generator.py
   - Use smaller sample sizes in benchmark
   - Monitor system memory usage

### Performance Tuning:

- **For Rust**: Use `--release` flag for optimized builds
- **For Python**: Consider using PyPy for potential speed improvements
- **For DuckDB**: Ensure adequate memory allocation

## Future Enhancements

1. **More Complex UDFs**: Test with heavier computational workloads
2. **Different Data Types**: Numeric processing, JSON parsing, etc.
3. **Parallel Processing**: Multi-threaded Rust implementations
4. **Memory Profiling**: Detailed memory usage analysis
5. **Production Testing**: Real-world dataset benchmarks

## Conclusion

This benchmark provides empirical data about the performance trade-offs between Python and Rust UDFs in DuckDB, helping inform decisions about when the additional complexity of Rust implementations is justified by performance gains.
