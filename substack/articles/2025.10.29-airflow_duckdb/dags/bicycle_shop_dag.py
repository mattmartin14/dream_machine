from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import duckdb

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Database file path
DB_PATH = '/Users/matthewmartin/dream_machine/substack/articles/2025.10.29-airflow_duckdb/bicycle_shop.duckdb'


def create_database():
    """Create DuckDB database and tables"""
    conn = duckdb.connect(DB_PATH)
    
    # Create order_hdr table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_hdr (
            order_id INTEGER PRIMARY KEY,
            customer_name VARCHAR,
            order_date DATE,
            total_amount DECIMAL(10,2),
            status VARCHAR
        )
    """)
    
    # Create order_dtl table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_dtl (
            detail_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_name VARCHAR,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            line_total DECIMAL(10,2),
            FOREIGN KEY (order_id) REFERENCES order_hdr(order_id)
        )
    """)
    
    conn.close()
    print("Database and tables created successfully!")


def insert_sample_data():
    """Insert sample bicycle shop data"""
    conn = duckdb.connect(DB_PATH)
    
    # Insert order headers
    conn.execute("""
        INSERT INTO order_hdr VALUES
        (1, 'John Smith', '2025-11-01', 1299.99, 'Completed'),
        (2, 'Sarah Johnson', '2025-11-02', 2499.98, 'Completed'),
        (3, 'Mike Brown', '2025-11-03', 899.50, 'Processing'),
        (4, 'Emily Davis', '2025-11-04', 3199.97, 'Shipped'),
        (5, 'Chris Wilson', '2025-11-05', 599.99, 'Completed')
    """)
    
    # Insert order details
    conn.execute("""
        INSERT INTO order_dtl VALUES
        -- Order 1
        (1, 1, 'Mountain Bike Pro', 1, 1199.99, 1199.99),
        (2, 1, 'Bike Helmet', 1, 49.99, 49.99),
        (3, 1, 'Water Bottle', 1, 15.00, 15.00),
        (4, 1, 'Bike Lock', 1, 35.01, 35.01),
        
        -- Order 2
        (5, 2, 'Road Bike Elite', 2, 1199.99, 2399.98),
        (6, 2, 'Cycling Shoes', 1, 99.99, 99.99),
        (7, 2, 'Bike Pump', 1, 0.01, 0.01),
        
        -- Order 3
        (8, 3, 'City Cruiser', 1, 799.99, 799.99),
        (9, 3, 'Basket', 1, 49.99, 49.99),
        (10, 3, 'Bell', 1, 9.99, 9.99),
        (11, 3, 'Kickstand', 1, 19.99, 19.99),
        (12, 3, 'Rear Rack', 1, 19.54, 19.54),
        
        -- Order 4
        (13, 4, 'Electric Bike', 1, 2999.99, 2999.99),
        (14, 4, 'Bike Cover', 1, 79.99, 79.99),
        (15, 4, 'Phone Mount', 1, 29.99, 29.99),
        (16, 4, 'Front Light', 1, 44.99, 44.99),
        (17, 4, 'Rear Light', 1, 45.01, 45.01),
        
        -- Order 5
        (18, 5, 'Kids Bike', 1, 299.99, 299.99),
        (19, 5, 'Training Wheels', 1, 49.99, 49.99),
        (20, 5, 'Kids Helmet', 1, 39.99, 39.99),
        (21, 5, 'Knee Pads', 1, 109.99, 109.99),
        (22, 5, 'Elbow Pads', 1, 100.03, 100.03)
    """)
    
    conn.close()
    print("Sample data inserted successfully!")


def query_data():
    """Query and display sample data"""
    conn = duckdb.connect(DB_PATH)
    
    print("\n=== Order Summary ===")
    result = conn.execute("""
        SELECT 
            h.order_id,
            h.customer_name,
            h.order_date,
            COUNT(d.detail_id) as item_count,
            SUM(d.line_total) as calculated_total,
            h.total_amount as header_total,
            h.status
        FROM order_hdr h
        LEFT JOIN order_dtl d ON h.order_id = d.order_id
        GROUP BY h.order_id, h.customer_name, h.order_date, h.total_amount, h.status
        ORDER BY h.order_date
    """).fetchall()
    
    for row in result:
        print(row)
    
    conn.close()


# Define the DAG
with DAG(
    'bicycle_shop_database',
    default_args=default_args,
    description='Create and populate bicycle shop database in DuckDB',
    schedule=None,  # Manual trigger only (changed from schedule_interval in Airflow 3.x)
    catchup=False,
    tags=['duckdb', 'bicycle_shop', 'demo'],
) as dag:
    
    create_db_task = PythonOperator(
        task_id='create_database_and_tables',
        python_callable=create_database,
    )
    
    insert_data_task = PythonOperator(
        task_id='insert_sample_data',
        python_callable=insert_sample_data,
    )
    
    query_task = PythonOperator(
        task_id='query_and_display_data',
        python_callable=query_data,
    )
    
    # Define task dependencies
    create_db_task >> insert_data_task >> query_task
