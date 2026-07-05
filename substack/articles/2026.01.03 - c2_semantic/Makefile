.PHONY: data dashboard help

help:
	@echo "Usage:"
	@echo "  make data            Run data refresh (default season year)"
	@echo "  make data YEAR=2025  Run data refresh for a specific year"
	@echo "  make dashboard       Launch the dashboard"

data:
	./refresh_c2_data.sh $(YEAR)

dashboard:
	./launch.sh
