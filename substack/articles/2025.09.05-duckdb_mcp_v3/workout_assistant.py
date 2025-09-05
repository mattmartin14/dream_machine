#!/usr/bin/env python3
"""
Workout Assistant - Natural Language Query Tool for DuckDB Workout Data

Usage: python workout_assistant.py "What was my average pace in 2025?"

To enable AI-powered SQL generation:
1. Set your OpenAI API key: export OPENAI_API_KEY="your-key-here"
2. Or set your Anthropic API key: export ANTHROPIC_API_KEY="your-key-here"
"""

import sys
import os
import duckdb
import pandas as pd
from typing import Optional

# Check for API keys
OPENAI_AVAILABLE = bool(os.getenv('OPENAI_API_KEY'))
ANTHROPIC_AVAILABLE = bool(os.getenv('ANTHROPIC_API_KEY'))

if OPENAI_AVAILABLE or ANTHROPIC_AVAILABLE:
    print("AI-powered SQL generation enabled!")
elif len(sys.argv) > 1:
    print("Using enhanced fallback SQL generation (no AI API key found)")
    print("Tip: Set OPENAI_API_KEY or ANTHROPIC_API_KEY environment variable for smarter queries")

# Database schema information for the AI model
SCHEMA_INFO = """
Database: c2_data.db
View: v_summary_workouts

Important Columns:
- Date: Date the workout was completed (datetime)
- Description: Type of workout (text)
- Work Time (Formatted): Total workout time in MM:SS.m format (text)
- Work Time (Seconds): Total workout time in seconds (numeric)
- Work Distance: Total meters accumulated (numeric)
- Pace: Average pace per 500 meters in MM:SS.m format (text)
- Type: Equipment type - RowErg or SkiErg (text)
- Stroke Rate/Cadence: Strokes per minute (numeric)
- Total Cal: Calories burned (numeric)
- Stroke Count: Total strokes (numeric)

Notes:
- Each row represents one workout
- Pace is measured as time per 500 meters (lower is faster)
- Distance is always in meters
- Use strftime('%Y', "Date") to extract year from dates
- Use double quotes around column names with spaces
"""

def generate_sql_with_openai(question: str) -> str:
    """Generate SQL query using OpenAI API"""
    try:
        import openai
        
        prompt = f"""
You are a SQL expert. Given this database schema and user question, generate a precise SQL query.

{SCHEMA_INFO}

User Question: {question}

Generate only the SQL query that answers this question. Do not include any explanations or markdown formatting.
The query should select from the view "v_summary_workouts".
Use proper column names with double quotes for names containing spaces.

SQL Query:"""

        client = openai.OpenAI()
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0.1
        )
        
        return response.choices[0].message.content.strip()
    except ImportError:
        return None
    except Exception as e:
        print(f"OpenAI API error: {e}")
        return None

def generate_sql_with_anthropic(question: str) -> str:
    """Generate SQL query using Anthropic Claude API"""
    try:
        import anthropic
        
        prompt = f"""
You are a SQL expert. Given this database schema and user question, generate a precise SQL query.

{SCHEMA_INFO}

User Question: {question}

Generate only the SQL query that answers this question. Do not include any explanations or markdown formatting.
The query should select from the view "v_summary_workouts".
Use proper column names with double quotes for names containing spaces.
"""

        client = anthropic.Anthropic()
        response = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return response.content[0].text.strip()
    except ImportError:
        return None
    except Exception as e:
        print(f"Anthropic API error: {e}")
        return None

def generate_sql_fallback(question: str) -> str:
    """Fallback SQL generation using simple keyword matching"""
    question_lower = question.lower()
    
    # Check for year ranges first (e.g., "2023 - 2025", "2023 to 2025", "2023-2025", "from 2024 to 2025")
    import re
    year_range_match = re.search(r'\b(20\d{2})\s*(?:[-–—]|to)\s*(20\d{2})\b', question_lower)
    if year_range_match:
        start_year = year_range_match.group(1)
        end_year = year_range_match.group(2)
        if "total" in question_lower and "distance" in question_lower:
            return f'SELECT SUM("Work Distance") AS total_distance, COUNT(*) AS workouts FROM v_summary_workouts WHERE strftime(\'%Y\', "Date") BETWEEN \'{start_year}\' AND \'{end_year}\';'
        else:
            return f'SELECT strftime(\'%Y\', "Date") AS year, COUNT(*) AS workouts, AVG("Work Distance") AS avg_distance, SUM("Work Distance") AS total_distance FROM v_summary_workouts WHERE strftime(\'%Y\', "Date") BETWEEN \'{start_year}\' AND \'{end_year}\' GROUP BY year ORDER BY year;'
    
    # Check for any single year (before general patterns)
    year_match = re.search(r'\b(20\d{2})\b', question_lower)
    if year_match:
        year = year_match.group(1)
        if "total" in question_lower and "distance" in question_lower:
            return f'SELECT SUM("Work Distance") AS total_distance, COUNT(*) AS workouts FROM v_summary_workouts WHERE strftime(\'%Y\', "Date") = \'{year}\';'
        else:
            return f'SELECT COUNT(*) AS workouts, AVG("Work Distance") AS avg_distance, SUM("Work Distance") AS total_distance FROM v_summary_workouts WHERE strftime(\'%Y\', "Date") = \'{year}\';'
    
    # Common patterns
    elif "longest" in question_lower and ("distance" in question_lower or "workout" in question_lower):
        return 'SELECT "Date", "Description", "Work Distance", "Work Time (Formatted)", "Type" FROM v_summary_workouts ORDER BY "Work Distance" DESC LIMIT 1;'
    
    elif "fastest" in question_lower and "pace" in question_lower:
        if "5k" in question_lower or "5000" in question_lower:
            return 'SELECT "Date", "Description", "Pace", "Work Distance", "Type" FROM v_summary_workouts WHERE "Work Distance" > 5000 ORDER BY "Pace" ASC LIMIT 1;'
        else:
            return 'SELECT "Date", "Description", "Pace", "Work Distance", "Type" FROM v_summary_workouts ORDER BY "Pace" ASC LIMIT 1;'
    
    elif "average" in question_lower and "distance" in question_lower:
        if "year" in question_lower:
            return 'SELECT strftime(\'%Y\', "Date") AS year, "Type", AVG("Work Distance") AS avg_distance, COUNT(*) AS workouts FROM v_summary_workouts GROUP BY year, "Type" ORDER BY year, "Type";'
        else:
            return 'SELECT "Type", AVG("Work Distance") AS avg_distance, COUNT(*) AS workouts FROM v_summary_workouts GROUP BY "Type";'
    
    elif "total" in question_lower and ("workout" in question_lower or "distance" in question_lower):
        return 'SELECT COUNT(*) AS total_workouts, SUM("Work Distance") AS total_distance FROM v_summary_workouts;'
    
    # Default: show recent workouts
    return 'SELECT "Date", "Description", "Work Distance", "Work Time (Formatted)", "Type" FROM v_summary_workouts ORDER BY "Date" DESC LIMIT 10;'

def execute_query(sql: str) -> Optional[pd.DataFrame]:
    """Execute SQL query against the database"""
    try:
        con = duckdb.connect('c2_data.db')
        result = con.execute(sql).df()
        con.close()
        return result
    except Exception as e:
        print(f"Database error: {e}")
        return None

def format_results(df: pd.DataFrame, question: str) -> str:
    """Format query results for display"""
    if df.empty:
        return "No results found."
    
    output = f"\nQuestion: {question}\n"
    output += "=" * (len(question) + 10) + "\n\n"
    
    # Format based on result type
    if len(df) == 1:
        # Single result - show as key-value pairs
        for col in df.columns:
            value = df[col].iloc[0]
            if pd.isna(value):
                continue
            if isinstance(value, float):
                if col.lower().find('distance') >= 0:
                    output += f"{col}: {value:,.0f} meters\n"
                elif col.lower().find('avg') >= 0:
                    output += f"{col}: {value:,.1f}\n"
                else:
                    output += f"{col}: {value:.2f}\n"
            else:
                output += f"{col}: {value}\n"
    else:
        # Multiple results - show as table
        output += df.to_string(index=False, float_format=lambda x: f'{x:,.1f}' if pd.notna(x) else '')
    
    return output

def main():
    if len(sys.argv) < 2:
        print("Usage: python workout_assistant.py \"Your question about workouts\"")
        print("\nExample questions:")
        print("  - What was my longest workout?")
        print("  - What's my fastest pace for 5k+ workouts?")
        print("  - How many workouts did I do in 2025?")
        print("  - What's my average distance by equipment type?")
        return
    
    question = " ".join(sys.argv[1:])
    print(f"Processing question: {question}")
    
    # Try to generate SQL using AI APIs first, then fallback
    sql = generate_sql_with_openai(question)
    if not sql:
        sql = generate_sql_with_anthropic(question)
    if not sql:
        print("Using fallback SQL generation...")
        sql = generate_sql_fallback(question)
    
    print(f"Generated SQL: {sql}")
    
    # Execute query
    result = execute_query(sql)
    if result is not None:
        print(format_results(result, question))
    else:
        print("Failed to execute query.")

if __name__ == "__main__":
    main()
