import unittest
import sys
import os
import json

# Add the src directory to path so we can import our parser
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from sql_parser import parse_sql


class TestSQLParser(unittest.TestCase):
    """Test cases for the SQL parser functionality."""

    def test_basic_join_with_aliases(self):
        """Test the example from project requirements."""
        sql = """
        select a.order_id, a.order_dt, b.sku_nbr, b.cost_amt, b.quantity
        from orders as a
            join order_details as b
                on a.order_id = b.order_id
        """
        
        result = json.loads(parse_sql(sql))
        
        # Check we have 2 tables
        self.assertEqual(len(result['tables']), 2)
        
        # Check orders table
        orders_table = next(t for t in result['tables'] if t['table_name'] == 'orders')
        self.assertEqual(orders_table['alias'], 'a')
        self.assertIn('order_id', orders_table['columns'])
        self.assertIn('order_dt', orders_table['columns'])
        
        # Check order_details table
        details_table = next(t for t in result['tables'] if t['table_name'] == 'order_details')
        self.assertEqual(details_table['alias'], 'b')
        self.assertIn('sku_nbr', details_table['columns'])
        self.assertIn('cost_amt', details_table['columns'])
        self.assertIn('quantity', details_table['columns'])

    def test_multiple_joins(self):
        """Test a query with multiple joins."""
        sql = """
        select c.customer_name, o.order_date, p.product_name, od.quantity
        from customers as c
            join orders as o on c.customer_id = o.customer_id
            join order_details as od on o.order_id = od.order_id
            join products as p on od.product_id = p.product_id
        """
        
        result = json.loads(parse_sql(sql))
        
        # Should have 4 tables
        self.assertEqual(len(result['tables']), 4)
        
        table_names = [t['table_name'] for t in result['tables']]
        self.assertIn('customers', table_names)
        self.assertIn('orders', table_names)
        self.assertIn('order_details', table_names)
        self.assertIn('products', table_names)

    def test_no_aliases(self):
        """Test query without table aliases."""
        sql = """
        select employees.name, employees.salary, departments.dept_name
        from employees
            join departments on employees.dept_id = departments.dept_id
        """
        
        result = json.loads(parse_sql(sql))
        
        # Should have 2 tables
        self.assertEqual(len(result['tables']), 2)
        
        # Check that aliases are None when not specified
        for table in result['tables']:
            self.assertIsNone(table['alias'])

    def test_mixed_alias_styles(self):
        """Test query with different alias styles (with and without 'AS')."""
        sql = """
        select u.username, u.email, prof.bio, prof.avatar_url
        from users u
            left join user_profiles as prof on u.user_id = prof.user_id
        """
        
        result = json.loads(parse_sql(sql))
        
        # Should have 2 tables
        self.assertEqual(len(result['tables']), 2)
        
        users_table = next(t for t in result['tables'] if t['table_name'] == 'users')
        profiles_table = next(t for t in result['tables'] if t['table_name'] == 'user_profiles')
        
        self.assertEqual(users_table['alias'], 'u')
        self.assertEqual(profiles_table['alias'], 'prof')

    def test_single_table_query(self):
        """Test a simple single-table query."""
        sql = """
        select id, name, email
        from users
        """
        
        result = json.loads(parse_sql(sql))
        
        # Should have 1 table
        self.assertEqual(len(result['tables']), 1)
        self.assertEqual(result['tables'][0]['table_name'], 'users')
        self.assertIsNone(result['tables'][0]['alias'])

    def test_right_join(self):
        """Test a query with RIGHT JOIN."""
        sql = """
        select a.name, b.description
        from table_a as a
            right join table_b as b on a.id = b.a_id
        """
        
        result = json.loads(parse_sql(sql))
        
        # Should have 2 tables
        self.assertEqual(len(result['tables']), 2)
        
        table_a = next(t for t in result['tables'] if t['table_name'] == 'table_a')
        table_b = next(t for t in result['tables'] if t['table_name'] == 'table_b')
        
        self.assertEqual(table_a['alias'], 'a')
        self.assertEqual(table_b['alias'], 'b')

    def test_complex_column_names(self):
        """Test handling of complex column references."""
        sql = """
        select 
            sales.region_id,
            sales.total_amount,
            regions.region_name,
            regions.country_code
        from sales_data as sales
            join geographic_regions as regions 
                on sales.region_id = regions.id
        """
        
        result = json.loads(parse_sql(sql))
        
        sales_table = next(t for t in result['tables'] if t['table_name'] == 'sales_data')
        regions_table = next(t for t in result['tables'] if t['table_name'] == 'geographic_regions')
        
        # Check that columns are properly assigned to tables
        self.assertIn('region_id', sales_table['columns'])
        self.assertIn('total_amount', sales_table['columns'])
        self.assertIn('region_name', regions_table['columns'])
        self.assertIn('country_code', regions_table['columns'])

    def test_case_insensitive_keywords(self):
        """Test that SQL keywords are handled case-insensitively."""
        sql = """
        SELECT a.field1, b.field2
        FROM Table1 AS a
            JOIN Table2 AS b ON a.id = b.id
        """
        
        result = json.loads(parse_sql(sql))
        
        # Should parse correctly despite mixed case
        self.assertEqual(len(result['tables']), 2)
        
        table1 = next(t for t in result['tables'] if t['table_name'] == 'Table1')
        table2 = next(t for t in result['tables'] if t['table_name'] == 'Table2')
        
        self.assertEqual(table1['alias'], 'a')
        self.assertEqual(table2['alias'], 'b')


def run_tests():
    """Run all test cases and display results."""
    print("Running SQL Parser Tests...")
    print("=" * 50)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestSQLParser)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 50)
    if result.wasSuccessful():
        print(f"✅ All {result.testsRun} tests passed!")
    else:
        print(f"❌ {len(result.failures)} test(s) failed, {len(result.errors)} error(s)")
        
    return result.wasSuccessful()


if __name__ == "__main__":
    run_tests()
