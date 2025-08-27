import re
import json
from typing import List, Dict, Any, Optional, Tuple


def parse_sql(sql_string: str) -> str:
    """
    Parse a SQL string to extract table names, aliases, and columns used.
    
    Args:
        sql_string (str): The SQL query string to parse
        
    Returns:
        str: JSON string containing tables with their aliases and columns
    """
    # Clean up the SQL string - remove extra whitespace and normalize
    cleaned_sql = _clean_sql(sql_string)
    
    # Extract table information (name, alias) from FROM and JOIN clauses
    tables_info = _extract_tables(cleaned_sql)
    
    # Extract column information from SELECT clause
    columns = _extract_columns(cleaned_sql)
    
    # Match columns to their respective tables based on aliases
    result = _match_columns_to_tables(tables_info, columns)
    
    return json.dumps(result, indent=2)


def _clean_sql(sql_string: str) -> str:
    """Clean and normalize the SQL string."""
    # Remove extra whitespace and normalize to lowercase for parsing
    # But preserve original case for output
    sql_string = re.sub(r'\s+', ' ', sql_string.strip())
    return sql_string


def _extract_tables(sql: str) -> List[Dict[str, str]]:
    """
    Extract table names and aliases from FROM and JOIN clauses.
    
    Returns:
        List of dictionaries containing table_name and alias
    """
    tables = []
    
    # Find FROM clause - two patterns: with AS and without AS
    from_pattern_with_as = r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+as\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    from_pattern_simple = r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:left|right|inner|outer|join|where|order|group|having|limit|$)'
    from_pattern_no_alias = r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s*$|\s+(?:join|where|order|group|having|limit|$))'
    
    # Try with AS first
    from_match = re.search(from_pattern_with_as, sql, re.IGNORECASE)
    if from_match:
        tables.append({
            'table_name': from_match.group(1),
            'alias': from_match.group(2)
        })
    else:
        # Try simple alias (no AS)
        from_match = re.search(from_pattern_simple, sql, re.IGNORECASE)
        if from_match:
            tables.append({
                'table_name': from_match.group(1),
                'alias': from_match.group(2)
            })
        else:
            # Try no alias
            from_match = re.search(from_pattern_no_alias, sql, re.IGNORECASE)
            if from_match:
                tables.append({
                    'table_name': from_match.group(1),
                    'alias': None
                })
    
    # Find JOIN clauses
    join_pattern_with_as = r'\b(?:left\s+|right\s+|inner\s+|outer\s+)?join\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+as\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+on'
    join_pattern_simple = r'\b(?:left\s+|right\s+|inner\s+|outer\s+)?join\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+on'
    join_pattern_no_alias = r'\b(?:left\s+|right\s+|inner\s+|outer\s+)?join\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+on'
    
    # Find all JOIN matches
    for pattern, has_alias in [(join_pattern_with_as, True), (join_pattern_simple, True), (join_pattern_no_alias, False)]:
        matches = re.findall(pattern, sql, re.IGNORECASE)
        for match in matches:
            if has_alias and len(match) >= 2:
                table_name = match[0]
                alias = match[1] if len(match) > 1 else None
            else:
                table_name = match if isinstance(match, str) else match[0]
                alias = None
            
            # Avoid duplicates
            if not any(t['table_name'] == table_name for t in tables):
                tables.append({
                    'table_name': table_name,
                    'alias': alias
                })
    
    return tables


def _extract_columns(sql: str) -> List[Tuple[str, Optional[str]]]:
    """
    Extract column references from SELECT clause.
    
    Returns:
        List of tuples (column_name, table_alias_or_name)
    """
    columns = []
    
    # Find the SELECT clause
    select_match = re.search(r'\bselect\s+(.*?)\s+from\b', sql, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return columns
    
    select_clause = select_match.group(1)
    
    # Split by commas but be careful of nested functions
    column_parts = _split_select_columns(select_clause)
    
    for column_part in column_parts:
        column_part = column_part.strip()
        
        # Handle table.column or alias.column references
        if '.' in column_part:
            # Split on the last dot to handle schema.table.column cases
            table_ref, column_name = column_part.rsplit('.', 1)
            table_ref = table_ref.strip()
            column_name = column_name.strip()
            columns.append((column_name, table_ref))
        else:
            # Column without table reference
            columns.append((column_part, None))
    
    return columns


def _split_select_columns(select_clause: str) -> List[str]:
    """Split SELECT clause by commas, handling nested parentheses."""
    columns = []
    current_column = ""
    paren_depth = 0
    
    for char in select_clause:
        if char == '(':
            paren_depth += 1
        elif char == ')':
            paren_depth -= 1
        elif char == ',' and paren_depth == 0:
            columns.append(current_column.strip())
            current_column = ""
            continue
        
        current_column += char
    
    # Don't forget the last column
    if current_column.strip():
        columns.append(current_column.strip())
    
    return columns


def _match_columns_to_tables(tables_info: List[Dict[str, str]], columns: List[Tuple[str, Optional[str]]]) -> Dict[str, Any]:
    """
    Match extracted columns to their respective tables.
    
    Args:
        tables_info: List of table dictionaries with name and alias
        columns: List of tuples (column_name, table_reference)
        
    Returns:
        Dictionary in the required JSON format
    """
    # Create a mapping from alias/name to table info
    alias_to_table = {}
    for table in tables_info:
        if table['alias']:
            alias_to_table[table['alias']] = table
        alias_to_table[table['table_name']] = table
    
    # Initialize result structure
    result = {"tables": []}
    table_columns = {}
    
    # Initialize each table with empty column list
    for table in tables_info:
        key = table['table_name']
        if key not in table_columns:
            table_columns[key] = set()
    
    # Match columns to tables
    for column_name, table_ref in columns:
        if table_ref and table_ref in alias_to_table:
            # Column has explicit table reference
            table_name = alias_to_table[table_ref]['table_name']
            table_columns[table_name].add(column_name)
        elif table_ref is None and len(tables_info) == 1:
            # Single table query - assign unqualified columns to the single table
            table_name = tables_info[0]['table_name']
            table_columns[table_name].add(column_name)
        # If table_ref is provided but not found, or multiple tables with unqualified column, skip
    
    # Build the final result
    for table in tables_info:
        table_name = table['table_name']
        table_result = {
            "table_name": table_name,
            "alias": table['alias'],
            "columns": sorted(list(table_columns.get(table_name, set())))
        }
        result["tables"].append(table_result)
    
    return result


if __name__ == "__main__":
    # Test with the example from the requirements
    test_sql = """
    select a.order_id, a.order_dt, b.sku_nbr, b.cost_amt, b.quantity
    from orders as a
        join order_details as b
            on a.order_id = b.order_id
    """
    
    result = parse_sql(test_sql)
    print("Test Result:")
    print(result)
