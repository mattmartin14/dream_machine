#!/usr/bin/env python3

import asyncio
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

@dataclass
class QueryIntent:
    """Represents the parsed intent of a natural language query"""
    action: str  # SELECT, COUNT, AVG, MAX, MIN, etc.
    target: str  # what we're looking for (distance, pace, time, etc.)
    filters: List[Dict]  # conditions to apply
    groupby: Optional[str] = None
    orderby: Optional[str] = None
    limit: Optional[int] = None
    aggregate: Optional[str] = None

class DynamicSQLGenerator:
    def __init__(self):
        # Database schema
        self.table_name = "summary_workouts"
        self.columns = {
            "date": "Date",
            "description": "Description", 
            "time": "Work Time (Formatted)",
            "distance": "Work Distance",
            "pace": "Pace",
            "type": "Type",
            "equipment": "Type"
        }
        
        # Mapping for common terms
        self.distance_terms = ["distance", "meters", "m", "far", "long"]
        self.time_terms = ["time", "duration", "minutes", "seconds"]
        self.pace_terms = ["pace", "split", "speed", "fast", "slow"]
        self.equipment_terms = ["equipment", "type", "machine", "rowerg", "skierg"]
        
        # Aggregation functions
        self.aggregations = {
            "best": "MIN",
            "fastest": "MIN", 
            "worst": "MAX",
            "slowest": "MAX",
            "longest": "MAX",
            "shortest": "MIN",
            "average": "AVG",
            "total": "SUM",
            "count": "COUNT"
        }

    def parse_natural_language(self, question: str) -> QueryIntent:
        """Parse natural language into structured query intent"""
        question = question.lower().strip()
        
        # Initialize intent
        intent = QueryIntent(action="SELECT", target="*", filters=[])
        
        # Detect aggregation/action
        for agg_word, sql_func in self.aggregations.items():
            if agg_word in question:
                intent.aggregate = sql_func
                intent.action = "SELECT"
                break
        
        # Detect what we're looking for (target)
        if any(term in question for term in self.distance_terms):
            intent.target = "distance"
        elif any(term in question for term in self.time_terms):
            intent.target = "time"
        elif any(term in question for term in self.pace_terms):
            intent.target = "pace"
        elif "workout" in question:
            intent.target = "workout"
        
        # Detect filters
        intent.filters = self._extract_filters(question)
        
        # Detect grouping
        if any(phrase in question for phrase in ["by equipment", "by type", "each equipment", "each type"]):
            intent.groupby = "Type"
        
        # Detect ordering
        if intent.aggregate:
            if intent.aggregate in ["MIN"] and intent.target in ["time", "pace"]:
                intent.orderby = f"{self.columns.get(intent.target, intent.target)} ASC"
            elif intent.aggregate in ["MAX"] and intent.target == "distance":
                intent.orderby = f"{self.columns.get(intent.target, intent.target)} DESC"
        elif "recent" in question or "latest" in question:
            intent.orderby = "Date DESC"
        elif "oldest" in question:
            intent.orderby = "Date ASC"
            
        # Detect limits
        if "recent" in question:
            intent.limit = 10
        elif any(word in question for word in ["best", "fastest", "longest"]) and not intent.groupby:
            intent.limit = 5
            
        return intent

    def _extract_filters(self, question: str) -> List[Dict]:
        """Extract filter conditions from the question"""
        filters = []
        
        # Distance-based filters
        distance_patterns = [
            (r"(\d+)k\b", lambda m: int(m.group(1)) * 1000),
            (r"(\d+)\s*(?:meter|metres|m)\b", lambda m: int(m.group(1))),
            (r"(\d+)\s*(?:kilometer|kilometres|km)\b", lambda m: int(m.group(1)) * 1000)
        ]
        
        for pattern, converter in distance_patterns:
            match = re.search(pattern, question)
            if match:
                target_distance = converter(match)
                # Add tolerance for distance matching (±100m)
                filters.append({
                    "column": "Work Distance",
                    "operator": ">=",
                    "value": target_distance - 100
                })
                filters.append({
                    "column": "Work Distance", 
                    "operator": "<=",
                    "value": target_distance + 100
                })
                break
        
        # Equipment type filters
        if "rowerg" in question:
            filters.append({
                "column": "Type",
                "operator": "=",
                "value": "'RowErg'"
            })
        elif "skierg" in question:
            filters.append({
                "column": "Type",
                "operator": "=", 
                "value": "'SkiErg'"
            })
            
        # Time period filters
        if "this year" in question:
            filters.append({
                "column": "Date",
                "operator": ">=",
                "value": "'2025-01-01'"
            })
        elif "last month" in question:
            filters.append({
                "column": "Date",
                "operator": ">=",
                "value": "DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)"
            })
            
        return filters

    def build_sql(self, intent: QueryIntent) -> str:
        """Dynamically build SQL from parsed intent"""
        
        # Build SELECT clause
        if intent.aggregate and intent.target != "*":
            if intent.target == "distance":
                select_parts = [f"{intent.aggregate}(\"Work Distance\") as {intent.aggregate.lower()}_distance"]
            elif intent.target == "time":
                select_parts = [f"{intent.aggregate}(\"Work Time (Formatted)\") as {intent.aggregate.lower()}_time"]
            elif intent.target == "pace":
                select_parts = [f"{intent.aggregate}(Pace) as {intent.aggregate.lower()}_pace"]
            else:
                select_parts = [f"{intent.aggregate}(*) as {intent.aggregate.lower()}_count"]
                
            # Add context columns when grouping
            if intent.groupby:
                select_parts.insert(0, intent.groupby)
            elif intent.aggregate in ["MIN", "MAX"]:
                # For best/worst queries, include context
                select_parts.extend([
                    "ANY_VALUE(Date) as date",
                    "ANY_VALUE(Description) as description",
                    "ANY_VALUE(\"Work Time (Formatted)\") as time",
                    "ANY_VALUE(\"Work Distance\") as distance"
                ])
        else:
            # Regular SELECT
            select_parts = ["Date", "Description", "\"Work Time (Formatted)\"", "\"Work Distance\"", "Pace", "Type"]
            
        select_clause = f"SELECT {', '.join(select_parts)}"
        
        # Build FROM clause
        from_clause = f"FROM {self.table_name}"
        
        # Build WHERE clause
        where_conditions = []
        for filter_cond in intent.filters:
            condition = f"\"{filter_cond['column']}\" {filter_cond['operator']} {filter_cond['value']}"
            where_conditions.append(condition)
            
        # Add data quality filters
        if intent.target == "pace" or "pace" in str(intent.aggregate).lower():
            where_conditions.append("Pace IS NOT NULL AND Pace != ''")
            
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
            
        # Build GROUP BY clause
        group_clause = ""
        if intent.groupby:
            group_clause = f"GROUP BY {intent.groupby}"
            
        # Build ORDER BY clause  
        order_clause = ""
        if intent.orderby:
            order_clause = f"ORDER BY {intent.orderby}"
        elif intent.groupby:
            order_clause = f"ORDER BY {intent.groupby}"
            
        # Build LIMIT clause
        limit_clause = ""
        if intent.limit:
            limit_clause = f"LIMIT {intent.limit}"
            
        # Combine all parts
        sql_parts = [select_clause, from_clause, where_clause, group_clause, order_clause, limit_clause]
        sql = " ".join(part for part in sql_parts if part)
        
        return sql

class SmartWorkoutQueryAssistant:
    def __init__(self, db_path="./c2_data.db"):
        self.db_path = db_path
        self.sql_generator = DynamicSQLGenerator()

    async def ask_question(self, question: str) -> str:
        """Ask a natural language question and get the answer via MCP server."""
        
        # Parse the natural language question
        intent = self.sql_generator.parse_natural_language(question)
        
        # Generate SQL dynamically
        sql_query = self.sql_generator.build_sql(intent)
        
        print(f"Question: {question}")
        print(f"Parsed Intent: {intent}")
        print(f"Generated SQL: {sql_query}")
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
                    
                    if result.content and len(result.content) > 0:
                        return result.content[0].text
                    else:
                        return "No results returned"
        
        except Exception as e:
            return f"Error: {str(e)}"

async def main():
    assistant = SmartWorkoutQueryAssistant()
    
    # Test dynamic SQL generation
    test_questions = [
        "What was my longest workout?",
        "What's my best 6k time?",
        "Show me my average distance by equipment type",
        "How many RowErg workouts have I done?",
        "What's my fastest pace on the SkiErg?",
        "Show me my recent 10k workouts",
        "What's my slowest 5k time?",
        "Count all my workouts by type"
    ]
    
    for question in test_questions:
        print(f"\n{'='*60}")
        result = await assistant.ask_question(question)
        print("Result:")
        print(result)
        print()

if __name__ == "__main__":
    asyncio.run(main())
