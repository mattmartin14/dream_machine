import duckdb
import pandas as pd

# Connect to the DuckDB database
con = duckdb.connect('c2_data.db')

print("="*60)
print("WORKOUT ANALYSIS RESULTS")
print("="*60)

# 1. Longest workout (distance and time)
longest_workout = con.execute('''
    SELECT "Date", "Description", "Work Time (Formatted)", "Work Distance", "Type", "Pace"
    FROM v_summary_workouts
    ORDER BY "Work Distance" DESC, "Work Time (Formatted)" DESC
    LIMIT 1;
''').df()
print('\n1. LONGEST WORKOUT (by distance):')
print(f"   Date: {longest_workout['Date'].iloc[0]}")
print(f"   Description: {longest_workout['Description'].iloc[0]}")
print(f"   Distance: {longest_workout['Work Distance'].iloc[0]:,} meters")
print(f"   Time: {longest_workout['Work Time (Formatted)'].iloc[0]}")
print(f"   Equipment: {longest_workout['Type'].iloc[0]}")
print(f"   Pace: {longest_workout['Pace'].iloc[0]}")

# 2. Fastest pace for workouts over 5k meters
fastest_pace = con.execute('''
    SELECT "Date", "Description", "Work Time (Formatted)", "Work Distance", "Type", "Pace"
    FROM v_summary_workouts
    WHERE "Work Distance" > 5000
    ORDER BY "Pace" ASC
    LIMIT 1;
''').df()
print('\n2. FASTEST PACE for workouts over 5k meters:')
print(f"   Date: {fastest_pace['Date'].iloc[0]}")
print(f"   Description: {fastest_pace['Description'].iloc[0]}")
print(f"   Distance: {fastest_pace['Work Distance'].iloc[0]:,} meters")
print(f"   Time: {fastest_pace['Work Time (Formatted)'].iloc[0]}")
print(f"   Equipment: {fastest_pace['Type'].iloc[0]}")
print(f"   Pace: {fastest_pace['Pace'].iloc[0]}")

# 3. Average distance by year and equipment type
avg_distance = con.execute('''
    SELECT
      strftime('%Y', "Date") AS year,
      "Type" AS equipment_type,
      AVG("Work Distance") AS avg_distance,
      COUNT(*) AS workout_count
    FROM v_summary_workouts
    GROUP BY year, equipment_type
    ORDER BY year, equipment_type;
''').df()
print('\n3. AVERAGE DISTANCE by year and equipment type:')
for _, row in avg_distance.iterrows():
    print(f"   {row['year']} - {row['equipment_type']}: {row['avg_distance']:,.0f} meters (from {row['workout_count']} workouts)")

print("\n" + "="*60)

con.close()
