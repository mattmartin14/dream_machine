SCRIPT_PATH="${C2_DL_SCRIPT:-${HOME}/dream_machine/concept2/c2_workouts_dl.py}"

# Determine default season year based on today's date.
# Seasons run from May 1 of year Y through April 30 of year Y+1,
# and are labeled by the ending year (Y+1).
today_year=$(date +%Y)
today_month=$(date +%m)

if [ "$today_month" -ge 5 ]; then
	default_season_year=$((today_year + 1))
else
	default_season_year=$today_year
fi

# Use explicit argument if provided, otherwise fall back to default.
SEASON_YEAR="${1:-$default_season_year}"

echo "Running data refresh for Concept2 season year: $SEASON_YEAR"

uv run "$SCRIPT_PATH" --season-year "$SEASON_YEAR"

# Examples:
#   ./refresh_c2_data.sh          # uses computed default season year
#   ./refresh_c2_data.sh 2026     # overrides with explicit year