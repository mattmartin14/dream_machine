#!/usr/bin/env python3

import sys
import os

# Add the src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from sql_parser import parse_sql

# Test the failing case
sql = """
select employees.name, employees.salary, departments.dept_name
from employees
    join departments on employees.dept_id = departments.dept_id
"""

print("Testing SQL:")
print(sql)
print("\nResult:")
result = parse_sql(sql)
print(result)
