#!/usr/bin/env python3

import sys
import os

# Add the src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from sql_parser import parse_sql

# Test the single table case
sql = """
select id, name, email
from users
"""

print("Testing SQL:")
print(sql)
print("\nResult:")
result = parse_sql(sql)
print(result)
