#!/usr/bin/env python3

import asyncio
import re
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

class WorkoutQueryAssistant:
    def __init__(self, db_path="./c2_data.db"):
        self.db_path = db_path
        self.table_schema = {
            "table_name": "summary_workouts",
            "columns": {
                "Date": "DATETIME - the date the workout was completed",
                "Description": "STRING - Type of workout (e.g., '6000m row', '40:44 row')",
                "Work Time (Formatted)": "STRING - total workout time in MM:SS.m format",
                "Work Distance": "NUMBER - Total meters accumulated for the exercise", 
                "Pace": "STRING - Average pace per 500 meters (split time)",
                "Type": "STRING - machine type (RowErg or SkiErg)"
            }
        }
    
    def natural_language_to_sql(self, question: str) -> str:
        """
        Convert natural language questions about workouts to SQL queries.
        This is a simple rule-based approach - in production, you'd use a more 
        sophisticated LLM or semantic parsing.
        """
        question = question.lower().strip()
        
        # Common patterns and their SQL translations
        if any(word in question for word in ["longest", "maximum distance", "farthest"]):
            return """
            SELECT Date, Description, "Work Time (Formatted)", "Work Distance", Type 
            FROM summary_workouts 
            ORDER BY "Work Distance" DESC 
            LIMIT 1
            """
        
        elif any(word in question for word in ["shortest", "minimum distance"]):
            return """
            SELECT Date, Description, "Work Time (Formatted)", "Work Distance", Type 
            FROM summary_workouts 
            ORDER BY "Work Distance" ASC 
            LIMIT 1
            """
        
        elif "6k" in question or "6000" in question:
            if "best" in question or "fastest" in question:
                return """
                SELECT Date, Description, "Work Time (Formatted)", "Work Distance", Pace, Type 
                FROM summary_workouts 
                WHERE "Work Distance" >= 5900 AND "Work Distance" <= 6100 
                ORDER BY "Work Time (Formatted)" ASC 
                LIMIT 5
                """
            else:
                return """
                SELECT Date, Description, "Work Time (Formatted)", "Work Distance", Pace, Type 
                FROM summary_workouts 
                WHERE "Work Distance" >= 5900 AND "Work Distance" <= 6100
                ORDER BY Date DESC
                """
        
        elif "10k" in question or "10000" in question:
            if "best" in question or "fastest" in question:
                return """
                SELECT Date, Description, "Work Time (Formatted)", "Work Distance", Pace, Type 
                FROM summary_workouts 
                WHERE "Work Distance" >= 9900 AND "Work Distance" <= 10100 
                ORDER BY "Work Time (Formatted)" ASC 
                LIMIT 5
                """
            else:
                return """
                SELECT Date, Description, "Work Time (Formatted)", "Work Distance", Pace, Type 
                FROM summary_workouts 
                WHERE "Work Distance" >= 9900 AND "Work Distance" <= 10100
                ORDER BY Date DESC
                """
        
        elif any(word in question for word in ["best pace", "fastest pace", "pace by equipment", "pace by type"]):
            return """
            SELECT Type, 
                   MIN(Pace) as best_pace,
                   ANY_VALUE(Date) as date_of_best,
                   ANY_VALUE(Description) as description,
                   ANY_VALUE("Work Time (Formatted)") as time,
                   ANY_VALUE("Work Distance") as distance
            FROM summary_workouts 
            WHERE Pace IS NOT NULL AND Pace != '' 
            GROUP BY Type 
            ORDER BY Type
            """
        
        elif "recent" in question or "latest" in question:
            return """
            SELECT Date, Description, "Work Time (Formatted)", "Work Distance", Pace, Type 
            FROM summary_workouts 
            ORDER BY Date DESC 
            LIMIT 10
            """
        
        elif "total" in question and ("distance" in question or "meters" in question):
            return """
            SELECT SUM("Work Distance") as total_distance,
                   COUNT(*) as total_workouts,
                   Type
            FROM summary_workouts 
            GROUP BY Type
            ORDER BY Type
            """
        
        elif "average" in question:
            if "distance" in question:
                return """
                SELECT Type, 
                       AVG("Work Distance") as avg_distance,
                       COUNT(*) as workout_count
                FROM summary_workouts 
                GROUP BY Type
                ORDER BY Type
                """
            elif "pace" in question:
                return """
                SELECT Type, 
                       AVG(CAST(REPLACE(Pace, ':', '.') AS FLOAT)) as avg_pace_numeric,
                       COUNT(*) as workout_count
                FROM summary_workouts 
                WHERE Pace IS NOT NULL AND Pace != ''
                GROUP BY Type
                ORDER BY Type
                """
        
        elif "count" in question or "how many" in question:
            if "rowerg" in question:
                return """
                SELECT COUNT(*) as rowerg_workouts
                FROM summary_workouts 
                WHERE Type = 'RowErg'
                """
            elif "skierg" in question:
                return """
                SELECT COUNT(*) as skierg_workouts
                FROM summary_workouts 
                WHERE Type = 'SkiErg'
                """
            else:
                return """
                SELECT Type, COUNT(*) as workout_count
                FROM summary_workouts 
                GROUP BY Type
                ORDER BY Type
                """
        
        # Default: show all data (limited)
        else:
            return """
            SELECT Date, Description, "Work Time (Formatted)", "Work Distance", Pace, Type 
            FROM summary_workouts 
            ORDER BY Date DESC 
            LIMIT 10
            """

    async def ask_question(self, question: str) -> str:
        """
        Ask a natural language question and get the answer via MCP server.
        """
        # Convert natural language to SQL
        sql_query = self.natural_language_to_sql(question)
        
        print(f"Question: {question}")
        print(f"Generated SQL: {sql_query.strip()}")
        print("=" * 50)
        
        # Execute via MCP server
        server_params = StdioServerParameters(
            command="uvx",
            args=["mcp-server-motherduck", "--transport", "stdio", "--db-path", self.db_path, "--read-only"]
        )
        
        try:
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    
                    result = await session.call_tool("query", {"query": sql_query})
                    
                    # Extract the text content from the result
                    if result.content and len(result.content) > 0:
                        return result.content[0].text
                    else:
                        return "No results returned"
        
        except Exception as e:
            return f"Error: {str(e)}"

async def main():
    assistant = WorkoutQueryAssistant()
    
    # Example questions
    questions = [
        "What was my longest workout?",
        "What was my best time for a 6k?", 
        "What was my best pace by equipment type?",
        "How many RowErg workouts have I done?",
        "What's my average distance for each equipment type?",
        "Show me my recent workouts"
    ]
    
    for question in questions:
        print(f"\n{'='*60}")
        result = await assistant.ask_question(question)
        print("Result:")
        print(result)
        print()

if __name__ == "__main__":
    asyncio.run(main())
