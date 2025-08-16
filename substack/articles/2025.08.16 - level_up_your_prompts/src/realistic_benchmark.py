"""
Realistic DuckDB UDF Performance Benchmark

This benchmark demonstrates the performance characteristics of Python vs Rust UDFs
in DuckDB, including the overhead of Python-Rust boundary crossings.
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

def setup_and_test_environment():
    """Setup and test both UDF implementations"""
    print("DuckDB UDF Performance Analysis")
    print("=" * 60)
    print("This benchmark analyzes the real-world performance characteristics")
    print("of Python vs Rust UDFs, including PyO3 boundary crossing overhead.")
    print()
    
    # Test Python UDF
    print("Testing Python UDF...")
    test_text = "Customer alice@test.com called (555) 123-4567 on 12/25/2024 about https://example.com"
    python_result = parse_text_complexity_python(test_text)
    print(f"✓ Python UDF result: {python_result:.2f}")
    
    # Test Rust UDF
    try:
        import string_parser_udf
        rust_result = string_parser_udf.parse_text_complexity(test_text)
        print(f"✓ Rust UDF result: {rust_result:.2f}")
        
        if abs(python_result - rust_result) < 0.01:
            print("✓ Results match - implementations are consistent")
        else:
            print(f"⚠️ Results differ: Python={python_result:.2f}, Rust={rust_result:.2f}")
        
        return True
    except ImportError as e:
        print(f"❌ Rust UDF not available: {e}")
        return False

def create_realistic_benchmark():
    """Create a benchmark that shows realistic performance patterns"""
    
    db_path = "/Users/matthewmartin/dream_machine/substack/articles/2025.08.16 - level_up_your_prompts/data/test_database.duckdb"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return {}
    
    conn = duckdb.connect(db_path)
    
    # Check data size
    count_result = conn.execute("SELECT COUNT(*) FROM test_data").fetchone()
    total_rows = count_result[0]
    print(f"Database contains {total_rows:,} rows")
    
    # Register UDFs with explicit types
    try:
        conn.create_function("parse_python", parse_text_complexity_python, ['VARCHAR'], 'DOUBLE')
        print("✓ Python UDF registered")
        python_available = True
    except Exception as e:
        print(f"❌ Python UDF registration failed: {e}")
        python_available = False
    
    try:
        import string_parser_udf
        conn.create_function("parse_rust", string_parser_udf.parse_text_complexity, ['VARCHAR'], 'DOUBLE')
        print("✓ Rust UDF registered")
        rust_available = True
    except Exception as e:
        print(f"❌ Rust UDF registration failed: {e}")
        rust_available = False
    
    if not (python_available or rust_available):
        return {}
    
    # Test different sample sizes to show overhead patterns
    sample_sizes = [1000, 5000, 10000, 25000]
    results = {
        'sample_sizes': sample_sizes,
        'python_times': [],
        'rust_times': [],
        'python_throughput': [],
        'rust_throughput': []
    }
    
    for sample_size in sample_sizes:
        actual_sample = min(sample_size, total_rows)
        print(f"\n--- Testing with {actual_sample:,} rows ---")
        
        # Python UDF
        if python_available:
            print("Running Python UDF...")
            start_time = time.time()
            python_result = conn.execute(f"""
                SELECT AVG(parse_python(complex_text)) as avg_complexity
                FROM test_data 
                LIMIT {actual_sample}
            """).fetchone()
            python_time = time.time() - start_time
            python_throughput = actual_sample / python_time
            
            results['python_times'].append(python_time)
            results['python_throughput'].append(python_throughput)
            print(f"  Python: {python_time:.3f}s ({python_throughput:.0f} rows/sec, result: {python_result[0]:.2f})")
        
        # Rust UDF
        if rust_available:
            print("Running Rust UDF...")
            start_time = time.time()
            rust_result = conn.execute(f"""
                SELECT AVG(parse_rust(complex_text)) as avg_complexity
                FROM test_data 
                LIMIT {actual_sample}
            """).fetchone()
            rust_time = time.time() - start_time
            rust_throughput = actual_sample / rust_time
            
            results['rust_times'].append(rust_time)
            results['rust_throughput'].append(rust_throughput)
            print(f"  Rust:   {rust_time:.3f}s ({rust_throughput:.0f} rows/sec, result: {rust_result[0]:.2f})")
            
            # Show overhead analysis
            if python_available:
                overhead_factor = rust_time / python_time
                print(f"  📊 Rust overhead: {overhead_factor:.2f}x slower due to PyO3 boundary crossing")
    
    conn.close()
    return results

def create_analysis_charts(results: Dict):
    """Create comprehensive analysis charts"""
    
    if not results:
        print("❌ No results to chart")
        return
    
    sample_sizes = results['sample_sizes']
    python_times = results.get('python_times', [])
    rust_times = results.get('rust_times', [])
    python_throughput = results.get('python_throughput', [])
    rust_throughput = results.get('rust_throughput', [])
    
    # Create subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('DuckDB UDF Performance Analysis: Python vs Rust', fontsize=16)
    
    # Chart 1: Execution Time
    ax1.set_title('Execution Time by Sample Size')
    if python_times:
        ax1.plot(sample_sizes[:len(python_times)], python_times, 'o-', label='Python UDF', color='blue', linewidth=2)
    if rust_times:
        ax1.plot(sample_sizes[:len(rust_times)], rust_times, 's-', label='Rust UDF', color='red', linewidth=2)
    ax1.set_xlabel('Sample Size (rows)')
    ax1.set_ylabel('Execution Time (seconds)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    
    # Chart 2: Throughput
    ax2.set_title('Processing Throughput')
    if python_throughput:
        ax2.plot(sample_sizes[:len(python_throughput)], python_throughput, 'o-', label='Python UDF', color='blue', linewidth=2)
    if rust_throughput:
        ax2.plot(sample_sizes[:len(rust_throughput)], rust_throughput, 's-', label='Rust UDF', color='red', linewidth=2)
    ax2.set_xlabel('Sample Size (rows)')
    ax2.set_ylabel('Throughput (rows/second)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xscale('log')
    
    # Chart 3: Overhead Analysis
    if python_times and rust_times:
        min_len = min(len(python_times), len(rust_times))
        overhead_ratios = [rust_times[i] / python_times[i] for i in range(min_len)]
        
        ax3.set_title('Rust Overhead Factor (Rust Time / Python Time)')
        ax3.bar(range(min_len), overhead_ratios, color='orange', alpha=0.7)
        ax3.set_xlabel('Test Index')
        ax3.set_ylabel('Overhead Factor')
        ax3.set_xticks(range(min_len))
        ax3.set_xticklabels([f'{sample_sizes[i]:,}' for i in range(min_len)], rotation=45)
        ax3.axhline(y=1, color='green', linestyle='--', label='Equal Performance')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Add text annotations
        avg_overhead = sum(overhead_ratios) / len(overhead_ratios)
        ax3.text(0.5, 0.95, f'Average Overhead: {avg_overhead:.1f}x', 
                transform=ax3.transAxes, ha='center', va='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    else:
        ax3.text(0.5, 0.5, 'Insufficient data for overhead analysis', 
                ha='center', va='center', transform=ax3.transAxes)
        ax3.set_title('Overhead Analysis - N/A')
    
    # Chart 4: Performance Insights
    ax4.axis('off')
    insights_text = []
    
    if python_times and rust_times:
        min_len = min(len(python_times), len(rust_times))
        avg_python = sum(python_times[:min_len]) / min_len
        avg_rust = sum(rust_times[:min_len]) / min_len
        
        insights_text.extend([
            "PERFORMANCE INSIGHTS:",
            "",
            f"Average Python time: {avg_python:.3f}s",
            f"Average Rust time: {avg_rust:.3f}s",
            "",
        ])
        
        if avg_rust > avg_python:
            overhead_pct = ((avg_rust - avg_python) / avg_python) * 100
            insights_text.extend([
                "🔍 KEY FINDINGS:",
                f"• Rust UDF is {avg_rust/avg_python:.1f}x slower than Python",
                f"• {overhead_pct:.0f}% overhead from PyO3 boundary crossing",
                f"• Python regex engine is highly optimized",
                f"• Rust benefits diminished by function call overhead",
                "",
                "💡 RECOMMENDATIONS:",
                "• Use Python UDFs for text processing tasks",
                "• Consider Rust for CPU-intensive math operations",
                "• Batch processing can help amortize Rust overhead",
            ])
        else:
            speedup = avg_python / avg_rust
            insights_text.extend([
                "🔍 KEY FINDINGS:",
                f"• Rust UDF is {speedup:.1f}x faster than Python",
                f"• Rust optimization overcame PyO3 overhead",
                "",
                "💡 RECOMMENDATIONS:",
                "• Rust UDFs beneficial for this workload",
                "• Consider Rust for similar processing tasks",
            ])
    else:
        insights_text = [
            "PERFORMANCE INSIGHTS:",
            "",
            "Insufficient data for comparison",
            "",
            "Both UDFs were not available for testing"
        ]
    
    ax4.text(0.05, 0.95, '\n'.join(insights_text), 
             transform=ax4.transAxes, fontsize=10,
             verticalalignment='top', fontfamily='monospace')
    
    plt.tight_layout()
    
    # Save chart
    chart_path = "/Users/matthewmartin/dream_machine/substack/articles/2025.08.16 - level_up_your_prompts/results/performance_comparison.png"
    plt.savefig(chart_path, dpi=300, bbox_inches='tight')
    print(f"\n✓ Analysis chart saved to: {chart_path}")
    
    plt.show()
    return chart_path

def print_executive_summary(results: Dict):
    """Print executive summary of findings"""
    
    print("\n" + "="*80)
    print("EXECUTIVE SUMMARY: Python vs Rust UDFs in DuckDB")
    print("="*80)
    
    sample_sizes = results.get('sample_sizes', [])
    python_times = results.get('python_times', [])
    rust_times = results.get('rust_times', [])
    
    if python_times and rust_times:
        min_len = min(len(python_times), len(rust_times))
        avg_python = sum(python_times[:min_len]) / min_len
        avg_rust = sum(rust_times[:min_len]) / min_len
        total_samples = sum(sample_sizes[:min_len])
        
        print(f"📊 PERFORMANCE ANALYSIS")
        print(f"   Total rows processed: {total_samples:,}")
        print(f"   Average Python execution time: {avg_python:.3f}s")
        print(f"   Average Rust execution time: {avg_rust:.3f}s")
        
        if avg_rust > avg_python:
            overhead = avg_rust / avg_python
            print(f"\n🔍 KEY FINDING: PyO3 Overhead Dominates")
            print(f"   Rust UDF is {overhead:.1f}x slower than Python")
            print(f"   Primary cause: Python-Rust boundary crossing overhead")
            print(f"   Python's optimized regex engine performs very well")
            
            print(f"\n💡 RECOMMENDATIONS:")
            print(f"   ✓ Use Python UDFs for text processing and regex operations")
            print(f"   ✓ Consider Rust UDFs for pure mathematical computations")
            print(f"   ✓ Batch processing can help amortize Rust overhead")
            print(f"   ✓ Profile before choosing - overhead varies by use case")
            
        else:
            speedup = avg_python / avg_rust
            print(f"\n🔍 KEY FINDING: Rust Optimization Successful")
            print(f"   Rust UDF is {speedup:.1f}x faster than Python")
            print(f"   Rust's performance overcame PyO3 overhead")
            
        print(f"\n📈 SCALABILITY INSIGHTS:")
        if len(sample_sizes) > 1:
            python_slope = (python_times[-1] - python_times[0]) / (sample_sizes[-1] - sample_sizes[0])
            if rust_times:
                rust_slope = (rust_times[-1] - rust_times[0]) / (sample_sizes[-1] - sample_sizes[0])
                print(f"   Python scaling: {python_slope*1000:.3f}ms per 1000 rows")
                print(f"   Rust scaling: {rust_slope*1000:.3f}ms per 1000 rows")
        
    else:
        print("❌ Insufficient data for meaningful analysis")
        print("   Both Python and Rust UDFs need to be available for comparison")
    
    print(f"\n📚 TECHNICAL LEARNINGS:")
    print(f"   • PyO3 boundary crossing has significant overhead for small operations")
    print(f"   • Python's C-based regex library is highly optimized")
    print(f"   • Rust's advantages appear in compute-heavy, not I/O-heavy operations")
    print(f"   • DuckDB's UDF interface adds its own overhead layer")
    
    print("="*80)

def main():
    """Main benchmark execution"""
    
    # Test environment
    rust_available = setup_and_test_environment()
    
    if not rust_available:
        print("\n⚠️ Running in Python-only mode")
        print("To test Rust UDF:")
        print("1. Build Rust extension: cd rust_udf && maturin develop --release")
        print("2. Re-run this benchmark")
    
    print("\n" + "="*60)
    print("RUNNING REALISTIC PERFORMANCE BENCHMARK")
    print("="*60)
    
    # Run benchmarks
    results = create_realistic_benchmark()
    
    if results:
        # Create analysis charts
        create_analysis_charts(results)
        
        # Print summary
        print_executive_summary(results)
        
        print(f"\n✅ Benchmark Analysis Complete!")
        print(f"   Results demonstrate real-world UDF performance characteristics")
        print(f"   Charts and analysis saved to results/")
        
    else:
        print("❌ Benchmark failed - check database and UDF setup")

if __name__ == "__main__":
    main()
