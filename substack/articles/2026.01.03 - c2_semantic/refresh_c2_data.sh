SCRIPT_PATH="${C2_DL_SCRIPT:-${HOME}/dream_machine/concept2/c2_workouts_dl.py}"
SEASON_YEAR="$1"

uv run "$SCRIPT_PATH" --season-year "$SEASON_YEAR"

# ./refresh_c2_data.sh 2026