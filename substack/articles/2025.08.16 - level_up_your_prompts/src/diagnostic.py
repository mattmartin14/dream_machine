"""
Simple diagnostic benchmark to identify performance and hanging issues
"""

import duckdb
import time
import sys
import os
from typing import List

# Add the source directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__)))

from python_udf import parse_text_complexity_python

def test_individual_functions():
    """Test both functions individually to compare raw performance"""
    
    print("Testing individual function performance...")
    
    test_text = "Customer John Smith with email john.smith@company.com called on 12/25/2024 about order #12345. Phone: (555) 123-4567. Website: https://example.com/support. Priority: High. Details: authentication issues with payment processing system requiring immediate debugging and configuration optimization analysis"
    
    # Test Python function
    print("Testing Python function:")
    python_times = []
    for i in range(5):
        start = time.time()
        for _ in range(1000):  # 1000 iterations
            result = parse_text_complexity_python(test_text)
        end = time.time()
        elapsed = end - start
        python_times.append(elapsed)
        print(f"  Run {i+1}: {elapsed:.4f}s (result: {result:.2f})")
    
    python_avg = sum(python_times) / len(python_times)
    print(f"Python average: {python_avg:.4f}s")
    
    # Test Rust function
    try:
        import string_parser_udf
        print("\nTesting Rust function:")
        rust_times = []
        for i in range(5):
            start = time.time()
            for _ in range(1000):  # 1000 iterations
                result = string_parser_udf.parse_text_complexity(test_text)
            end = time.time()
            elapsed = end - start
            rust_times.append(elapsed)
            print(f"  Run {i+1}: {elapsed:.4f}s (result: {result:.2f})")
        
        rust_avg = sum(rust_times) / len(rust_times)
        print(f"Rust average: {rust_avg:.4f}s")
        
        if python_avg > rust_avg:
            speedup = python_avg / rust_avg
            print(f"Rust is {speedup:.2f}x faster than Python")
        else:
            slowdown = rust_avg / python_avg
            print(f"Python is {slowdown:.2f}x faster than Rust (unexpected!)")
            
    except Exception as e:
        print(f"Error testing Rust function: {e}")

def test_duckdb_integration():
    """Test DuckDB integration with smaller datasets"""
    
    print("\n" + "="*50)
    print("Testing DuckDB Integration")
    print("="*50)
    
    # Create small test database
    conn = duckdb.connect(':memory:')
    
    # Create test data
    test_data = [
        "Customer Alice with email alice@test.com called on 1/15/2025. Phone: 555-123-4567",
        "Transaction #TX123 for $99.99 processed. User: bob@company.org, IP: 192.168.1.1",
        "API call to /api/users returned 200. Duration: 150ms. Client: dev@startup.io",
        "Support ticket #456 from charlie@business.net. Category: Technical issue",
        "Log entry [2025-01-15 10:30:00] - System maintenance completed successfully"
    ]
    
    # Insert test data
    conn.execute("CREATE TABLE test (id INTEGER, text VARCHAR)")
    for i, text in enumerate(test_data):
        conn.execute("INSERT INTO test VALUES (?, ?)", (i+1, text))
    
    print(f"Created test table with {len(test_data)} rows")
    
    # Test Python UDF
    try:
        conn.create_function("parse_python", parse_text_complexity_python, ['VARCHAR'], 'DOUBLE')
        print("✓ Python UDF registered")
        
        start_time = time.time()
        result = conn.execute("SELECT AVG(parse_python(text)) as avg_score FROM test").fetchone()
        python_time = time.time() - start_time
        print(f"Python UDF: {python_time:.4f}s (avg_score: {result[0]:.2f})")
        
    except Exception as e:
        print(f"❌ Python UDF error: {e}")
    
    # Test Rust UDF
    try:
        import string_parser_udf
        conn.create_function("parse_rust", string_parser_udf.parse_text_complexity, ['VARCHAR'], 'DOUBLE')
        print("✓ Rust UDF registered")
        
        start_time = time.time()
        result = conn.execute("SELECT AVG(parse_rust(text)) as avg_score FROM test").fetchone()
        rust_time = time.time() - start_time
        print(f"Rust UDF: {rust_time:.4f}s (avg_score: {result[0]:.2f})")
        
        # Test with timeout to detect hanging
        print("Testing potential hanging with larger query...")
        start_time = time.time()
        
        # Create larger dataset for hanging test
        conn.execute("CREATE TABLE large_test AS SELECT id, text FROM test, generate_series(1, 1000)")
        result = conn.execute("SELECT COUNT(*) FROM large_test").fetchone()
        print(f"Created large test table with {result[0]} rows")
        
        # Test with timeout mechanism
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError("Query timed out")
        
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(10)  # 10-second timeout
        
        try:
            start_time = time.time()
            result = conn.execute("SELECT AVG(parse_rust(text)) as avg_score FROM large_test LIMIT 500").fetchone()
            elapsed = time.time() - start_time
            print(f"Rust UDF on larger dataset: {elapsed:.4f}s (avg_score: {result[0]:.2f})")
            signal.alarm(0)  # Cancel alarm
        except TimeoutError:
            print("❌ Rust UDF query timed out - likely hanging")
            signal.alarm(0)
        except Exception as e:
            print(f"❌ Rust UDF error on larger dataset: {e}")
            signal.alarm(0)
            
    except Exception as e:
        print(f"❌ Rust UDF error: {e}")
    
    conn.close()

def analyze_rust_overhead():
    """Analyze potential Rust overhead issues"""
    
    print("\n" + "="*50)
    print("Analyzing Rust Overhead")
    print("="*50)
    
    try:
        import string_parser_udf
        
        # Test with different input sizes
        base_text = "email@test.com phone: 123-456-7890 "
        
        for multiplier in [1, 10, 100]:
            test_text = base_text * multiplier
            print(f"\nTesting with text length: {len(test_text)} characters")
            
            # Python timing
            start = time.time()
            for _ in range(100):
                python_result = parse_text_complexity_python(test_text)
            python_time = time.time() - start
            
            # Rust timing
            start = time.time()
            for _ in range(100):
                rust_result = string_parser_udf.parse_text_complexity(test_text)
            rust_time = time.time() - start
            
            print(f"  Python: {python_time:.4f}s (result: {python_result:.2f})")
            print(f"  Rust:   {rust_time:.4f}s (result: {rust_result:.2f})")
            
            if python_time < rust_time:
                overhead = (rust_time - python_time) / python_time * 100
                print(f"  ⚠️ Rust has {overhead:.1f}% overhead")
            else:
                speedup = python_time / rust_time
                print(f"  ✓ Rust is {speedup:.2f}x faster")
    
    except Exception as e:
        print(f"❌ Error in Rust overhead analysis: {e}")

if __name__ == "__main__":
    print("DuckDB UDF Diagnostic Tool")
    print("=" * 50)
    
    test_individual_functions()
    test_duckdb_integration() 
    analyze_rust_overhead()
    
    print("\n" + "="*50)
    print("Diagnostic complete!")
