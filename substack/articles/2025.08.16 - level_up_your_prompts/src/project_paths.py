"""
Common utilities for the DuckDB UDF Performance Project

This module provides common paths and utilities used across all scripts.
"""

import os

def get_project_root():
    """Get the project root directory using environment variables"""
    home = os.environ.get('HOME', '')
    if not home:
        # Fallback for systems without HOME environment variable
        home = os.path.expanduser('~')
    
    project_root = os.path.join(home, 'dream_machine/substack/articles/2025.08.16 - level_up_your_prompts')
    return project_root

def get_db_path():
    """Get the path to the test database"""
    return os.path.join(get_project_root(), 'data/test_database.duckdb')

def get_results_path():
    """Get the path to the results directory"""
    return os.path.join(get_project_root(), 'results')

def get_chart_path():
    """Get the path for the performance comparison chart"""
    return os.path.join(get_results_path(), 'performance_comparison.png')

def get_rust_udf_path():
    """Get the path to the Rust UDF directory"""
    return os.path.join(get_project_root(), 'rust_udf')

def ensure_directory_exists(path):
    """Ensure a directory exists, create it if it doesn't"""
    os.makedirs(path, exist_ok=True)
