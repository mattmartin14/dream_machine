I have a duckdb database called c2_data.db

### database contents

The database contains a view called v_summary_workouts. This table contains rows showing workout information for a concept 2 rower or skierg.

### Important table notes
Each row is considered a workout


### table column notes
Below are some details of important columns in this table:

Date - the date the workout was completed
Description - Type of workout that was ran; can be a fixed distance or time in most cases
Work Time (Formatted) - the total workout time in MM:SS.m
Work Distance - Total meters accumulated for the exercise
Pace - Average pace measured in time per 500 meters accumlated; this is known in the rowing world as the split time
Tyoe - indicates the type of machine used; can be either a RowErg or SkiErg


### Questions I want answered

Can you answer the following questions from my data:

1. What was my longest workout in terms of distance as well as time
2. What was my fastest pace ever recorded for a workout higher than 5k meters
3. What was my average distance by year and equipment type