import duckdb
import random
from datetime import datetime, timedelta
from decimal import Decimal

def generate_bicycle_orders(cn: duckdb.DuckDBPyConnection, target_rows=5000):
    
    # Sample data for generation
    bike_types = [
        'Mountain Bike - Trek X-Caliber', 'Road Bike - Specialized Allez',
        'Electric Bike - Cannondale Synapse NEO', 'Hybrid Bike - Giant Escape 3',
        'Gravel Bike - Surly Cross-Check', 'Kids Bike - Schwinn Elm',
        'Carbon Road Bike - Cervelo R5', 'Fat Bike - Salsa Mukluk',
        'Cyclocross Bike - Kona Jake', 'Touring Bike - Jamis Aurora Elite',
        'BMX Bike - Mongoose Legion', 'Folding Bike - Dahon Mariner',
        'Recumbent Bike - HP Velotechnik', 'Tandem Bike - Co-Motion Periscope'
    ]
    
    customer_names = [
        'John Smith', 'Sarah Johnson', 'Mike Davis', 'Emily Brown', 'David Wilson',
        'Jessica Garcia', 'Chris Miller', 'Amanda Martinez', 'Ryan Anderson', 'Lisa Taylor',
        'Kevin Thomas', 'Michelle Jackson', 'Daniel White', 'Jennifer Harris', 'Matthew Martin',
        'Ashley Thompson', 'Andrew Garcia', 'Stephanie Rodriguez', 'Joshua Lewis', 'Nicole Walker'
    ]
    
    # Generate email addresses from names
    def generate_email(name):
        first, last = name.lower().split()
        domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'company.com']
        return f"{first}.{last}@{random.choice(domains)}"
    
    # Drop existing table and create new one with customer info
    cn.execute("DROP TABLE IF EXISTS bicycle_orders")
    
    # Generate orders
    orders = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(target_rows):
        order_date = start_date + timedelta(days=random.randint(0, 365))
        po_number = f"PO-2024-{i+1:06d}"
        amount = round(random.uniform(200, 5000), 2)
        quantity = random.randint(1, 5)
        description = random.choice(bike_types)
        customer_name = random.choice(customer_names)
        customer_email = generate_email(customer_name)
        
        orders.append((
            order_date.strftime('%Y-%m-%d'),
            po_number,
            amount,
            quantity,
            description,
            customer_name,
            customer_email
        ))
    
    # Create table with new schema
    cn.execute("""
        CREATE TABLE bicycle_orders (
            order_date DATE,
            po_number VARCHAR,
            amount DECIMAL(10,2),
            quantity INTEGER,
            description VARCHAR,
            customer_name VARCHAR,
            customer_email VARCHAR
        )
    """)
    
    # Insert data in batches
    batch_size = 1000
    for i in range(0, len(orders), batch_size):
        batch = orders[i:i+batch_size]
        cn.executemany(
            "INSERT INTO bicycle_orders VALUES (?, ?, ?, ?, ?, ?, ?)",
            batch
        )
    
    #print(f"Generated {target_rows} bicycle orders")
    #return connection.sql("SELECT COUNT(*) as total_orders FROM bicycle_orders")
