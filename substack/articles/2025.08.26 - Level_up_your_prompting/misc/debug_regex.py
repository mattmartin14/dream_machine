#!/usr/bin/env python3

import re

sql = """
select u.username, u.email, prof.bio, prof.avatar_url
from users u
    left join user_profiles as prof on u.user_id = prof.user_id
"""

print("Testing FROM regex:")
from_pattern = r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:as\s+([a-zA-Z_][a-zA-Z0-9_]*)|(?:\s+([a-zA-Z_][a-zA-Z0-9_]*))(?=\s+(?:left|right|inner|outer|join|where|order|group|having|limit|$)))?'
from_match = re.search(from_pattern, sql, re.IGNORECASE)

if from_match:
    print(f"Full match: '{from_match.group(0)}'")
    print(f"Group 1 (table): '{from_match.group(1)}'")
    print(f"Group 2 (alias with AS): '{from_match.group(2) if from_match.group(2) else 'None'}'")
    print(f"Group 3 (alias without AS): '{from_match.group(3) if from_match.group(3) else 'None'}'")
else:
    print("No match found!")

print("\nTesting simpler pattern:")
simple_pattern = r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
simple_match = re.search(simple_pattern, sql, re.IGNORECASE)
if simple_match:
    print(f"Table: '{simple_match.group(1)}', Alias: '{simple_match.group(2)}'")
else:
    print("No simple match!")
