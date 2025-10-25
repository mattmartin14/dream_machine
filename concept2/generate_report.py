#!/usr/bin/env python3
"""
Concept2 Workout Report Generator
Generates a PDF report with rowing pace over time and monthly total meters charts.
"""

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
import matplotlib.patheffects as pe
import matplotlib.transforms as transforms
from matplotlib.backends.backend_pdf import PdfPages
from scipy import stats
import argparse
from datetime import datetime
import subprocess
import sys
import os

def run_data_download(season_year=2026):
    """Run the c2_workouts_dl.py script to download/update workout data"""
    print(f"Downloading workout data for season year {season_year}...")
    
    try:
        # Check if the script exists
        script_path = "c2_workouts_dl.py"
        if not os.path.exists(script_path):
            print(f"Warning: {script_path} not found in current directory")
            return False
            
        # Run the download script
        result = subprocess.run([
            sys.executable, script_path, 
            "--season-year", str(season_year)
        ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
        
        if result.returncode == 0:
            print("Workout data download completed successfully")
            if result.stdout:
                print("Download output:", result.stdout.strip())
            return True
        else:
            print(f"Error downloading workout data (exit code {result.returncode})")
            if result.stderr:
                print("Error details:", result.stderr.strip())
            return False
            
    except subprocess.TimeoutExpired:
        print("Error: Workout data download timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"Error running workout download script: {e}")
        return False

def setup_database():
    """Setup DuckDB connection and create views"""
    cn = duckdb.connect()
    cn.sql("create or replace view v_detail_workouts as select * from read_csv_auto('~/concept2/workouts/*detail*.csv')")
    cn.sql("create or replace view v_summary_workouts as select * from read_csv_auto('~/concept2/workouts/*summary*.csv')")
    return cn

def create_pace_chart(cn, equipment_type):
    """Create the rowing pace over time chart"""
    sql_pace = f"""
        select "Log ID" as workout_id
            ,cast("Date" as date) as workout_dt
            ,"Work Distance" as total_meters
            ,cast('00:' || "Pace" as time) as avg_split
        from v_summary_workouts
        where 1=1
            and lower(Type) = '{equipment_type}'
            and "Pace" is not null
            and "Work Distance" >= 1000
            and cast("Date" as date) >= date('2024-07-01')
        order by workout_dt
    """
    
    df_pace = cn.sql(sql_pace).df()
    
    if df_pace.empty:
        print("Warning: No pace data found")
        return None
    
    # Convert avg_split to seconds for plotting
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
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 8))
    
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
    
    # Add connecting line
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

    if equipment_type.lower() == 'rowerg':

        ax.set_title('RowErg Pace Over Time (Color-coded by Distance)')
    else:
        ax.set_title('SkiErg Pace Over Time (Color-coded by Distance)')
    ax.grid(True, alpha=0.3)
    
    # Add trend line
    if len(df_pace) > 1:
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
    
    plt.tight_layout()
    return fig

def create_monthly_meters_chart(cn):
    """Create the stacked bar chart showing total meters by month"""
    sql = """
        select year("Date") as workout_year
            ,type as machine_type
            ,month("Date") as workout_month
            ,sum("Work Distance") as total_meters
        from v_summary_workouts
        group by all
    """
    
    # Query and prepare data
    df = cn.sql(sql).df()
    
    if df.empty:
        print("Warning: No monthly meters data found")
        return None
        
    df['year_month'] = df['workout_year'].astype(str) + '-' + df['workout_month'].astype(str).str.zfill(2)
    
    # Pivot to get one column per machine_type and rows per year_month
    pivot = (
        df.sort_values(['workout_year', 'workout_month'])
          .pivot_table(index='year_month', columns='machine_type', values='total_meters', aggfunc='sum')
          .fillna(0)
    )
    
    # Ensure chronological order on the x-axis
    order = pd.to_datetime(pivot.index, format='%Y-%m').argsort()
    pivot = pivot.iloc[order]
    
    x = pivot.index.tolist()
    fig = plt.figure(figsize=(14, 8))
    ax = fig.gca()
    
    # Stacked bars by machine type
    bottom = None
    for col in pivot.columns:
        ax.bar(x, pivot[col], bottom=bottom, label=col, zorder=2)
        bottom = pivot[col] if bottom is None else bottom + pivot[col]
    
    plt.xlabel('Year-Month')
    plt.ylabel('Total Meters')
    plt.title('Total Meters by Year and Month — stacked by Machine Type')
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Average reference lines per year
    total_by_month = pivot.sum(axis=1)
    ymax = total_by_month.max()
    years_series = pd.to_datetime(pivot.index, format='%Y-%m').year
    years_list = years_series.tolist()
    avg_by_year = total_by_month.groupby(years_series).mean()
    
    # Draw dashed line segment spanning each year range
    for yr in avg_by_year.index:
        idxs = [i for i, yv in enumerate(years_list) if yv == yr]
        if not idxs:
            continue
        yavg = avg_by_year.loc[yr]
        ax.plot([x[idxs[0]], x[idxs[-1]]], [yavg, yavg],
                color='red', linestyle=(0, (4, 3)), linewidth=1.5, alpha=0.6,
                zorder=1, label=f'Avg {yr}: {yavg:,.0f}')
    
    # Improve readability: grid, y-axis formatting, headroom
    ax.yaxis.grid(True, linestyle='--', alpha=0.4)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, p: f'{v:,.0f}'))
    ax.set_ylim(0, ymax * 1.20)  # extra headroom for two-line labels
    
    # Thin x-axis labels if too many
    labels = ax.get_xticklabels()
    if len(labels) > 24:
        step = max(1, len(labels)//18)
        for i, label in enumerate(labels):
            label.set_visible(i % step == 0)
    
    plt.legend()
    
    # Lifetime total annotation in the upper-left corner
    lifetime_total = int(total_by_month.sum())
    ax.text(
        0.01, 0.99,
        f"Lifetime Total: {lifetime_total:,}",
        transform=ax.transAxes,
        ha='left', va='top',
        fontsize=10, fontweight='bold', color='white', zorder=5,
        bbox=dict(facecolor='black', edgecolor='none', alpha=0.75, boxstyle='round,pad=0.2')
    )
    
    # Annotate: two-line label above the bar top using fixed point offsets
    for i, y in enumerate(total_by_month):
        yr = years_list[i]
        yavg = avg_by_year.loc[yr]
        delta = y - yavg
    
        # Top line: total
        t_total = ax.annotate(
            f"{int(y):,}",
            xy=(x[i], y),
            xytext=(0, 18),  # points above bar top
            textcoords='offset points',
            ha='center', va='bottom',
            fontsize=9, fontweight='bold', zorder=4,
        )
        t_total.set_path_effects([pe.withStroke(linewidth=3, foreground='white')])
    
        # Second line: delta directly below the total (vs. year average)
        color = 'green' if delta >= 0 else 'red'
        t_delta = ax.annotate(
            f"{delta:+,.0f}",
            xy=(x[i], y),
            xytext=(0, 4),  # below the total label
            textcoords='offset points',
            ha='center', va='bottom',
            fontsize=9, color=color, style='italic', zorder=4,
        )
        t_delta.set_path_effects([pe.withStroke(linewidth=2, foreground='white')])
    
    return fig

def generate_pdf_report(output_filename="concept2_workout_report.pdf", season_year=2026, fetch_data=True):
    """Generate the complete PDF report"""
    # Optionally download/update the workout data
    if fetch_data:
        download_success = run_data_download(season_year)
        if not download_success:
            print("Warning: Data download failed, proceeding with existing data...")
    else:
        print("Skipping data download (--no-logbook-fetch specified)")
    
    print("Setting up database connection...")
    cn = setup_database()
    
    print("Generating PDF report...")
    with PdfPages(output_filename) as pdf:
        # Add title page
        fig = plt.figure(figsize=(8.5, 11))
        plt.axis('off')
        plt.text(0.5, 0.7, 'Concept2 Workout Report', 
                ha='center', va='center', fontsize=24, fontweight='bold')
        plt.text(0.5, 0.6, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}', 
                ha='center', va='center', fontsize=12)
        plt.text(0.5, 0.4, 'This report contains your rowing performance analysis including:\n\n• Pace progression over time\n• Monthly training volume by machine type', 
                ha='center', va='center', fontsize=12)
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
        
        # Generate and save pace chart
        print("Creating pace over time chart...")
        pace_fig = create_pace_chart(cn, "rowerg")
        if pace_fig:
            print("Saving pace chart to PDF...")
            pdf.savefig(pace_fig, bbox_inches='tight')
            plt.close(pace_fig)
        else:
            print("ERROR: Could not create pace chart!")

        pace_fig2 = create_pace_chart(cn, "skierg")
        if pace_fig2:
            print("Saving pace chart to PDF...")
            pdf.savefig(pace_fig2, bbox_inches='tight')
            plt.close(pace_fig2)
        else:
            print("ERROR: Could not create pace chart!")
        
        # Generate and save monthly meters chart
        print("Creating monthly meters chart...")
        meters_fig = create_monthly_meters_chart(cn)
        if meters_fig:
            pdf.savefig(meters_fig, bbox_inches='tight')
            plt.close(meters_fig)
    
    print(f"PDF report generated successfully: {output_filename}")
    return output_filename

def main():
    parser = argparse.ArgumentParser(description='Generate Concept2 workout report PDF')
    parser.add_argument('-o', '--output', default='concept2_workout_report.pdf',
                       help='Output PDF filename (default: concept2_workout_report.pdf)')
    parser.add_argument('-y', '--season-year', type=int, default=2026,
                       help='Season year to download data for (default: 2026)')
    parser.add_argument('--no-logbook-fetch', action='store_true',
                       help='Skip downloading workout data from Concept2 logbook')
    
    args = parser.parse_args()
    
    try:
        generate_pdf_report(args.output, args.season_year, fetch_data=not args.no_logbook_fetch)
    except Exception as e:
        print(f"Error generating report: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
