

def get_pace_over_time():
    """Create and return the pace over time chart figure for PDF generation"""
    sql_pace = """
        select "Log ID" as workout_id
            ,cast("Date" as date) as workout_dt
            ,"Work Distance" as total_meters
            ,cast('00:' || "Pace" as time) as avg_split
        from read_csv_auto('~/concept2/workouts/*summary*.csv')
        where 1=1
            and Type = 'RowErg'
            and "Pace" is not null
            and "Work Distance" >= 1000
        order by workout_dt
    """

    import matplotlib.pyplot as plt
    import pandas as pd
    from matplotlib.dates import DateFormatter
    import matplotlib.dates as mdates
    import duckdb

    try:
        # Get the data
        df_pace = duckdb.sql(sql_pace).df()

        if df_pace.empty:
            print("Warning: No pace data found")
            return None

        # Convert avg_split to seconds for plotting (easier to work with)
        df_pace['pace_seconds'] = pd.to_timedelta(df_pace['avg_split'].astype(str)).dt.total_seconds()

        # Add color coding based on distance ranges
        def get_color_for_distance(distance):
            if distance < 5000:
                return 'blue'
            elif 5000 <= distance <= 5999:
                return 'green'
            elif 6000 <= distance <= 6999:
                return 'orange'
            elif 7000 <= distance <= 7999:
                return 'red'
            else:  # >= 8000
                return 'purple'

        df_pace['color'] = df_pace['total_meters'].apply(get_color_for_distance)

        # Create the plot - use plt.ioff() to prevent display
        plt.ioff()  # Turn off interactive mode
        fig, ax = plt.subplots(figsize=(12, 8))  # Make it larger for PDF

        # Plot points with different colors for each distance range
        distance_ranges = [
            ('< 5000m', 'blue'),
            ('5000-5999m', 'green'), 
            ('6000-6999m', 'orange'),
            ('7000-7999m', 'red'),
            ('≥ 8000m', 'purple')
        ]

        for label, color in distance_ranges:
            mask = df_pace['color'] == color
            if mask.any():
                ax.plot(df_pace.loc[mask, 'workout_dt'], df_pace.loc[mask, 'pace_seconds'], 
                        marker='o', linewidth=0, markersize=6, color=color, label=label, alpha=0.7)

        # Add connecting line (in gray to show progression)
        ax.plot(df_pace['workout_dt'], df_pace['pace_seconds'], linewidth=1, color='gray', alpha=0.5, zorder=0)

        # Format y-axis to show pace as MM:SS
        def format_pace(seconds, pos):
            mins = int(seconds // 60)
            secs = int(seconds % 60)
            return f'{mins}:{secs:02d}'

        ax.yaxis.set_major_formatter(plt.FuncFormatter(format_pace))

        # Format x-axis dates
        ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        plt.xticks(rotation=45)

        # Labels and title
        ax.set_xlabel('Workout Date')
        ax.set_ylabel('Average Pace (MM:SS)')
        ax.set_title('Rowing Pace Over Time (Color-coded by Distance)')
        ax.grid(True, alpha=0.3)

        # Add trend line
        from scipy import stats
        if len(df_pace) > 1:
            # Convert dates to numeric for trend calculation
            x_numeric = mdates.date2num(df_pace['workout_dt'])
            slope, intercept, r_value, p_value, std_err = stats.linregress(x_numeric, df_pace['pace_seconds'])
            trend_line = slope * x_numeric + intercept
            ax.plot(df_pace['workout_dt'], trend_line, '--', color='black', alpha=0.7, linewidth=2, label=f'Trend (R²={r_value**2:.3f})')

        # Add legend
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

        # Add average pace annotations for each distance range
        pace_stats_text = "Average Pace by Distance:\n"
        for label, color in distance_ranges:
            mask = df_pace['color'] == color
            if mask.any():
                avg_pace_seconds = df_pace.loc[mask, 'pace_seconds'].mean()
                avg_pace_formatted = format_pace(avg_pace_seconds, 0)
                count = mask.sum()
                pace_stats_text += f"  {label}: {avg_pace_formatted} ({count} workouts)\n"

        # Add the pace stats text box below the legend
        ax.text(1.05, 0.4, pace_stats_text.strip(), transform=ax.transAxes,
                verticalalignment='top', fontsize=9,
                bbox=dict(boxstyle='round,pad=0.5', facecolor='lightblue', alpha=0.8))

        # Apply tight layout but don't show - just return the figure
        plt.tight_layout()
        
        print(f"DEBUG: Successfully created figure: {fig}")
        print(f"DEBUG: Figure type: {type(fig)}")
        return fig
        
    except Exception as e:
        print(f"ERROR in get_pace_over_time: {e}")
        import traceback
        traceback.print_exc()
        return None

# Test if this code runs when imported
print("DEBUG: get_pace_over_time_chart.py imported successfully")# print(f"Total workouts: {len(df_pace)}")
# print(f"Date range: {df_pace['workout_dt'].min()} to {df_pace['workout_dt'].max()}")
# print(f"Best pace: {format_pace(df_pace['pace_seconds'].min(), 0)}")
# print(f"Average pace: {format_pace(df_pace['pace_seconds'].mean(), 0)}")

# # Show distance distribution
# print("\nDistance distribution:")
# for label, color in distance_ranges:
#     mask = df_pace['color'] == color
#     count = mask.sum()
#     if count > 0:
#         print(f"  {label}: {count} workouts")