#!/usr/bin/env python3

import sys
import os

# Add the src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from sql_parser import parse_sql

# Test the failing mixed alias case
sql = """
select u.username, u.email, prof.bio, prof.avatar_url
from users u
    left join user_profiles as prof on u.user_id = prof.user_id
"""

print("Testing SQL:")
print(sql)
print("\nResult:")
result = parse_sql(sql)
print(result)
