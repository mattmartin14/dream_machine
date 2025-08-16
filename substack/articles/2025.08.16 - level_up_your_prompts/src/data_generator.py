"""
Data Generator for DuckDB UDF Performance Testing

This script creates a large dataset with complex strings for testing
the performance difference between Python and Rust-based UDFs.
"""

import duckdb
import random
import string
from typing import List
import os
import string
from typing import List
import os

def generate_complex_text() -> str:
    """Generate complex text with various patterns for realistic parsing scenarios."""
    
    # Base text templates with various patterns
    templates = [
        "Customer {name} with email {email} called on {date} about order #{order_id}. "
        "Phone: {phone}. Website: {url}. Priority: {priority}. "
        "Details: {description}",
        
        "Transaction ID: {tx_id} processed for ${amount} on {date}. "
        "User: {email}, IP: {ip}, Phone: {phone}. "
        "Reference URL: {url}. Status: {status}. Notes: {notes}",
        
        "Log entry [{timestamp}] - User {user_id} accessed {url} "
        "from IP {ip}. Response time: {response_time}ms. "
        "User-Agent contains: {user_agent}. Error count: {error_count}. "
        "Additional data: {extra_data}",
        
        "Support ticket #{ticket_id} from {email} on {date}. "
        "Category: {category}. Phone: {phone}. "
        "Description: {long_description}. "
        "Follow-up URL: {url}. Priority score: {score}.",
        
        "API call to {endpoint} at {timestamp} returned {response_code}. "
        "Client: {email}, Token: {token}, Duration: {duration}ms. "
        "Data payload size: {payload_size} bytes. "
        "Debug info: {debug_info}. Success rate: {success_rate}%"
    ]
    
    # Generate realistic data
    names = ["John Smith", "Sarah Johnson", "Mike Wilson", "Emma Davis", "Chris Brown", "Lisa Garcia", "David Miller", "Jessica Wilson"]
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "company.com", "business.org", "enterprise.net"]
    
    # Helper functions for generating realistic data
    def random_email():
        name = ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 12)))
        domain = random.choice(domains)
        return f"{name}@{domain}"
    
    def random_phone():
        formats = [
            f"{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            f"({random.randint(100,999)}) {random.randint(100,999)}-{random.randint(1000,9999)}"
        ]
        return random.choice(formats)
    
    def random_url():
        protocols = ["https://", "http://"]
        domains = ["example.com", "test.org", "demo.net", "sample.io", "api.service.com"]
        paths = ["", "/api/v1/data", "/dashboard", "/user/profile", "/admin/settings", "/public/docs"]
        return f"{random.choice(protocols)}{random.choice(domains)}{random.choice(paths)}"
    
    def random_date():
        year = random.randint(2020, 2025)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        formats = [f"{month}/{day}/{year}", f"{year}-{month:02d}-{day:02d}"]
        return random.choice(formats)
    
    def random_long_text():
        words = ["processing", "analysis", "optimization", "configuration", "implementation", 
                "debugging", "monitoring", "authentication", "authorization", "validation",
                "encryption", "compression", "serialization", "deserialization", "transformation"]
        return ' '.join(random.choices(words, k=random.randint(10, 25)))
    
    def random_ip():
        return f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
    
    # Choose random template and fill it
    template = random.choice(templates)
    
    data = {
        'name': random.choice(names),
        'email': random_email(),
        'phone': random_phone(),
        'url': random_url(),
        'date': random_date(),
        'order_id': random.randint(10000, 99999),
        'priority': random.choice(['High', 'Medium', 'Low', 'Critical']),
        'description': random_long_text(),
        'tx_id': ''.join(random.choices(string.ascii_uppercase + string.digits, k=12)),
        'amount': f"{random.randint(10, 9999)}.{random.randint(10, 99)}",
        'ip': random_ip(),
        'status': random.choice(['Success', 'Failed', 'Pending', 'Processing']),
        'notes': random_long_text(),
        'timestamp': f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
        'user_id': random.randint(1000, 9999),
        'response_time': random.randint(50, 2000),
        'user_agent': f"Browser/{random.randint(1,10)}.{random.randint(0,9)} System/{random.randint(1,20)}",
        'error_count': random.randint(0, 10),
        'extra_data': random_long_text(),
        'ticket_id': random.randint(100000, 999999),
        'category': random.choice(['Technical', 'Billing', 'General', 'Bug Report', 'Feature Request']),
        'long_description': ' '.join([random_long_text() for _ in range(3)]),
        'score': random.randint(1, 100),
        'endpoint': f"/api/v{random.randint(1,3)}/{random.choice(['users', 'orders', 'products', 'analytics'])}",
        'response_code': random.choice([200, 201, 400, 404, 500, 503]),
        'token': ''.join(random.choices(string.ascii_letters + string.digits, k=32)),
        'duration': random.randint(100, 5000),
        'payload_size': random.randint(1024, 1048576),
        'debug_info': random_long_text(),
        'success_rate': random.randint(85, 100)
    }
    
    try:
        return template.format(**data)
    except KeyError:
        # Fallback if template has missing keys
        return f"Sample text with email {data['email']} and phone {data['phone']} on {data['date']} - {data['description']}"

def create_database_and_table(db_path: str, num_rows: int = 100_000_000):
    """Create DuckDB database with test data."""
    
    print(f"Creating database with {num_rows:,} rows...")
    print("This may take several minutes for 100M rows...")
    
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    
    # Create the table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS test_data (
            id INTEGER,
            complex_text VARCHAR,
            created_date DATE
        )
    """)
    
    # For 100M rows, we'll use batch inserts to avoid memory issues
    batch_size = 100_000
    batches = num_rows // batch_size
    
    print(f"Inserting data in {batches} batches of {batch_size:,} rows each...")
    
    for batch_num in range(batches):
        # Generate batch data
        batch_data = []
        for i in range(batch_size):
            row_id = batch_num * batch_size + i + 1
            text = generate_complex_text()
            date = f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            batch_data.append((row_id, text, date))
        
        # Insert batch
        conn.executemany(
            "INSERT INTO test_data (id, complex_text, created_date) VALUES (?, ?, ?)",
            batch_data
        )
        
        if (batch_num + 1) % 10 == 0:
            print(f"Completed {batch_num + 1:,}/{batches:,} batches ({((batch_num + 1) * batch_size):,} rows)")
    
    # Handle remaining rows if any
    remaining = num_rows % batch_size
    if remaining > 0:
        batch_data = []
        for i in range(remaining):
            row_id = batches * batch_size + i + 1
            text = generate_complex_text()
            date = f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            batch_data.append((row_id, text, date))
        
        conn.executemany(
            "INSERT INTO test_data (id, complex_text, created_date) VALUES (?, ?, ?)",
            batch_data
        )
    
    # Verify data
    result = conn.execute("SELECT COUNT(*) as count FROM test_data").fetchone()
    print(f"Successfully created database with {result[0]:,} rows")
    
    # Show sample data
    print("\nSample data:")
    sample = conn.execute("SELECT id, complex_text FROM test_data LIMIT 3").fetchall()
    for row in sample:
        print(f"ID {row[0]}: {row[1][:100]}...")
    
    conn.close()

if __name__ == "__main__":
    from project_paths import get_db_path
    db_path = get_db_path()
    
    # For development/testing, start with fewer rows and increase as needed
    # Change this to 100_000_000 for the full test
    num_rows = 100_000  # Start with 100K for initial development
    
    create_database_and_table(db_path, num_rows)
    print(f"Database created at: {db_path}")
