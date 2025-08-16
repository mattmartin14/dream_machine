"""
DuckDB UDF Performance Benchmark

This script compares the performance of pure Python UDFs vs Rust-based UDFs
when processing large datasets in DuckDB.
"""

import duckdb
import time
import sys
import os
import matplotlib.pyplot as plt
import numpy as np
from typing import List, Tuple, Dict
import statistics

# Add the source directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__)))

# Import our Python UDF
from python_udf import parse_text_complexity_python

def setup_python_environment():
    """Setup Python dependencies if needed."""
    try:
        import duckdb
        import matplotlib.pyplot as plt
        import numpy as np
        print("✓ All Python dependencies are available")
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Please install required packages:")
        print("pip install duckdb matplotlib numpy pandas")
        return False

def build_rust_udf():
    """Build the Rust UDF using maturin."""
    from project_paths import get_rust_udf_path
    rust_dir = get_rust_udf_path()
    
    print("Building Rust UDF...")
    
    # Change to rust directory and build
    original_cwd = os.getcwd()
    try:
        os.chdir(rust_dir)
        
        # Install maturin if not available
        os.system("pip install maturin")
        
        # Build the Rust extension
        result = os.system("maturin develop --release")
        
        if result == 0:
            print("✓ Rust UDF built successfully")
            return True
        else:
            print("❌ Failed to build Rust UDF")
            return False
            
    finally:
        os.chdir(original_cwd)

def test_rust_import():
    """Test if we can import the Rust UDF."""
    try:
        import string_parser_udf
        print("✓ Rust UDF import successful")
        
        # Test the function
        test_result = string_parser_udf.parse_text_complexity("Test email: test@example.com phone: 123-456-7890")
        print(f"✓ Rust UDF test result: {test_result:.2f}")
        return True
    except ImportError as e:
        print(f"❌ Failed to import Rust UDF: {e}")
        return False

def register_udfs(conn: duckdb.DuckDBPyConnection) -> Tuple[bool, bool]:
    """Register both Python and Rust UDFs with DuckDB."""
    
    python_success = False
    rust_success = False
    
    # Register Python UDF
    try:
        conn.create_function("parse_text_python", parse_text_complexity_python, ['VARCHAR'], 'DOUBLE')
        print("✓ Python UDF registered successfully")
        python_success = True
    except Exception as e:
        print(f"❌ Failed to register Python UDF: {e}")
    
    # Register Rust UDF
    try:
        import string_parser_udf
        conn.create_function("parse_text_rust", string_parser_udf.parse_text_complexity, ['VARCHAR'], 'DOUBLE')
        print("✓ Rust UDF registered successfully")
        rust_success = True
    except Exception as e:
        print(f"❌ Failed to register Rust UDF: {e}")
        print("Make sure the Rust UDF was built successfully")
    
    return python_success, rust_success

def run_benchmark_query(conn: duckdb.DuckDBPyConnection, udf_name: str, limit: int = 100000) -> float:
    """Run a single benchmark query and return execution time."""
    
    query = f"""
    SELECT 
        AVG({udf_name}(complex_text)) as avg_complexity,
        COUNT(*) as row_count
    FROM test_data 
    LIMIT {limit}
    """
    
    start_time = time.time()
    result = conn.execute(query).fetchone()
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(f"  {udf_name}: {execution_time:.3f}s (avg_complexity: {result[0]:.2f}, rows: {result[1]:,})")
    
    return execution_time

def run_benchmarks(db_path: str, num_runs: int = 5, sample_size: int = 100000) -> Dict[str, List[float]]:
    """Run performance benchmarks comparing Python vs Rust UDFs."""
    
    print(f"\nRunning benchmarks with {sample_size:,} rows, {num_runs} runs each...")
    
    conn = duckdb.connect(db_path)
    
    # Verify data exists
    count_result = conn.execute("SELECT COUNT(*) FROM test_data").fetchone()
    print(f"Database contains {count_result[0]:,} rows")
    
    if count_result[0] < sample_size:
        print(f"Warning: Requested {sample_size:,} rows but only {count_result[0]:,} available")
        sample_size = min(sample_size, count_result[0])
    
    # Register UDFs
    python_success, rust_success = register_udfs(conn)
    
    if not python_success and not rust_success:
        print("❌ No UDFs could be registered. Exiting.")
        return {}
    
    results = {
        'python': [],
        'rust': []
    }
    
    # Run benchmarks
    for run_num in range(num_runs):
        print(f"\n--- Run {run_num + 1}/{num_runs} ---")
        
        if python_success:
            try:
                python_time = run_benchmark_query(conn, "parse_text_python", sample_size)
                results['python'].append(python_time)
            except Exception as e:
                print(f"❌ Python UDF run failed: {e}")
        
        if rust_success:
            try:
                rust_time = run_benchmark_query(conn, "parse_text_rust", sample_size)
                results['rust'].append(rust_time)
            except Exception as e:
                print(f"❌ Rust UDF run failed: {e}")
    
    conn.close()
    return results

def create_performance_chart(results: Dict[str, List[float]], sample_size: int):
    """Create a bar chart showing performance comparison."""
    
    print("\nCreating performance chart...")
    
    # Prepare data
    python_times = results.get('python', [])
    rust_times = results.get('rust', [])
    
    if not python_times and not rust_times:
        print("❌ No benchmark results to plot")
        return
    
    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Chart 1: Individual run times
    x_positions = []
    run_times = []
    colors = []
    labels = []
    
    run_numbers = range(1, max(len(python_times), len(rust_times)) + 1)
    
    for i, run_num in enumerate(run_numbers):
        if i < len(python_times):
            x_positions.append(run_num - 0.2)
            run_times.append(python_times[i])
            colors.append('skyblue')
            labels.append('Python' if i == 0 else "")
        
        if i < len(rust_times):
            x_positions.append(run_num + 0.2)
            run_times.append(rust_times[i])
            colors.append('lightcoral')
            labels.append('Rust' if i == 0 else "")
    
    bars1 = ax1.bar(x_positions, run_times, width=0.4, color=colors, alpha=0.8)
    ax1.set_xlabel('Run Number')
    ax1.set_ylabel('Execution Time (seconds)')
    ax1.set_title(f'UDF Performance Comparison\\n({sample_size:,} rows per run)')
    ax1.set_xticks(list(run_numbers))
    ax1.grid(True, alpha=0.3)
    
    # Add legend
    if python_times:
        ax1.bar([], [], color='skyblue', alpha=0.8, label='Python UDF')
    if rust_times:
        ax1.bar([], [], color='lightcoral', alpha=0.8, label='Rust UDF')
    ax1.legend()
    
    # Chart 2: Average performance with error bars
    avg_data = []
    std_data = []
    chart_labels = []
    chart_colors = []
    
    if python_times:
        avg_data.append(statistics.mean(python_times))
        std_data.append(statistics.stdev(python_times) if len(python_times) > 1 else 0)
        chart_labels.append('Python UDF')
        chart_colors.append('skyblue')
    
    if rust_times:
        avg_data.append(statistics.mean(rust_times))
        std_data.append(statistics.stdev(rust_times) if len(rust_times) > 1 else 0)
        chart_labels.append('Rust UDF')
        chart_colors.append('lightcoral')
    
    bars2 = ax2.bar(chart_labels, avg_data, yerr=std_data, capsize=5, 
                    color=chart_colors, alpha=0.8, edgecolor='black')
    ax2.set_ylabel('Average Execution Time (seconds)')
    ax2.set_title(f'Average Performance\\n({len(python_times)} Python + {len(rust_times)} Rust runs)')
    ax2.grid(True, alpha=0.3)
    
    # Add value labels on bars
    for bar, avg_time, std_time in zip(bars2, avg_data, std_data):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + std_time + 0.01,
                f'{avg_time:.3f}s\\n±{std_time:.3f}s',
                ha='center', va='bottom')
    
    plt.tight_layout()
    
    # Save chart
    from project_paths import get_chart_path
    chart_path = get_chart_path()
    plt.savefig(chart_path, dpi=300, bbox_inches='tight')
    print(f"✓ Chart saved to: {chart_path}")
    
    plt.show()
    
    return chart_path

def print_performance_summary(results: Dict[str, List[float]], sample_size: int):
    """Print a summary of benchmark results."""
    
    print("\\n" + "="*60)
    print("PERFORMANCE BENCHMARK SUMMARY")
    print("="*60)
    
    python_times = results.get('python', [])
    rust_times = results.get('rust', [])
    
    print(f"Sample size: {sample_size:,} rows per run")
    print(f"Number of runs: Python={len(python_times)}, Rust={len(rust_times)}")
    
    if python_times:
        python_avg = statistics.mean(python_times)
        python_std = statistics.stdev(python_times) if len(python_times) > 1 else 0
        python_min = min(python_times)
        python_max = max(python_times)
        
        print(f"\\nPython UDF Results:")
        print(f"  Average: {python_avg:.3f}s (±{python_std:.3f}s)")
        print(f"  Range: {python_min:.3f}s - {python_max:.3f}s")
        print(f"  All runs: {[f'{t:.3f}s' for t in python_times]}")
    
    if rust_times:
        rust_avg = statistics.mean(rust_times)
        rust_std = statistics.stdev(rust_times) if len(rust_times) > 1 else 0
        rust_min = min(rust_times)
        rust_max = max(rust_times)
        
        print(f"\\nRust UDF Results:")
        print(f"  Average: {rust_avg:.3f}s (±{rust_std:.3f}s)")
        print(f"  Range: {rust_min:.3f}s - {rust_max:.3f}s")
        print(f"  All runs: {[f'{t:.3f}s' for t in rust_times]}")
    
    if python_times and rust_times:
        python_avg = statistics.mean(python_times)
        rust_avg = statistics.mean(rust_times)
        
        if rust_avg < python_avg:
            speedup = python_avg / rust_avg
            print(f"\\nPerformance Improvement:")
            print(f"  Rust is {speedup:.2f}x faster than Python")
            print(f"  Time savings: {python_avg - rust_avg:.3f}s per query ({(python_avg - rust_avg)/python_avg*100:.1f}%)")
        else:
            slowdown = rust_avg / python_avg
            print(f"\\nPerformance Comparison:")
            print(f"  Python is {slowdown:.2f}x faster than Rust")
            print(f"  (Unexpected result - Rust overhead may be higher than computation benefit)")
    
    print("="*60)

def main():
    """Main benchmark execution function."""
    
    print("DuckDB UDF Performance Benchmark")
    print("=" * 50)
    
    # Check Python dependencies
    if not setup_python_environment():
        return
    
    # Build Rust UDF
    if not build_rust_udf():
        print("Warning: Rust UDF build failed. Will only test Python UDF.")
    
    # Test Rust import
    test_rust_import()
    
    # Database path
    from project_paths import get_db_path
    db_path = get_db_path()
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        print("Please run data_generator.py first to create the test database.")
        return
    
    # Run benchmarks with different sample sizes for testing
    # Start small and increase
    sample_sizes = [10000, 50000]  # Start with smaller samples for development
    
    for sample_size in sample_sizes:
        print(f"\\n{'='*60}")
        print(f"BENCHMARKING WITH {sample_size:,} ROWS")
        print(f"{'='*60}")
        
        results = run_benchmarks(db_path, num_runs=5, sample_size=sample_size)
        
        if results:
            print_performance_summary(results, sample_size)
            chart_path = create_performance_chart(results, sample_size)
        else:
            print("❌ No benchmark results obtained")
    
    print("\\n✓ Benchmark complete!")

if __name__ == "__main__":
    main()
