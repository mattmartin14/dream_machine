I have an mcp server for motherduck that i've launched in the terminal. it is using my duckdb database "c2_data.db"

### database contents

The database contains a table called summary_workouts. This table contains rows showing workout information for a concept 2 rower or skierg.

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
