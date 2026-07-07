YEAR ?= $(shell python3 -c "import datetime; t=datetime.date.today(); print(t.year+1 if t.month>=5 else t.year)")

.PHONY: data dashboard stop help

help:
	@echo "Usage:"
	@echo "  make data              Refresh data for the current season year ($(YEAR))"
	@echo "  make data YEAR=2025    Refresh data for a specific year"
	@echo "  make dashboard         Launch the dashboard at http://localhost:8080"
	@echo "  make stop              Stop the dashboard"

data:
	uv run python c2_workouts_dl.py --season-year $(YEAR)

dashboard:
	@lsof -ti:8080 | xargs kill 2>/dev/null || true
	@lsof -ti:8000 | xargs kill 2>/dev/null || true
	@pkill -f "dev.py" 2>/dev/null || true
	@sleep 1
	@./launch.sh

stop:
	@lsof -ti:8080 | xargs kill 2>/dev/null || true
	@lsof -ti:8000 | xargs kill 2>/dev/null || true
	@pkill -f "dev.py" 2>/dev/null || true
	@echo "Dashboard stopped."
