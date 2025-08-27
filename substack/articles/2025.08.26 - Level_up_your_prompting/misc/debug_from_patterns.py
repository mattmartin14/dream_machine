#!/usr/bin/env python3

import re

sql = """
select employees.name, employees.salary, departments.dept_name
from employees
    join departments on employees.dept_id = departments.dept_id
"""

print("Testing various FROM patterns:")

patterns = [
    r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s*$|\s+(?:where|order|group|having|limit|$))',
    r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+(?:join|where|order|group|having|limit|$))',
    r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(?=join|where|order|group|having|limit|$)',
    r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)(?=\s+(?:join|where|order|group|having|limit|$))'
]

for i, pattern in enumerate(patterns, 1):
    print(f"\nPattern {i}: {pattern}")
    match = re.search(pattern, sql, re.IGNORECASE)
    if match:
        print(f"  Match: '{match.group(1)}'")
    else:
        print("  No match")

print(f"\nSQL to test:\n{sql}")
