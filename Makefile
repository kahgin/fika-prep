VENV := .venv
PYTHON := $(VENV)/bin/python
HF_CACHE := $(CURDIR)/.hf_cache

all: sync

venv:
	@uv venv --clear $(VENV)

sync: venv
	@uv lock
	@uv sync 

sync-prod: venv
	@uv lock
	@uv sync --no-dev

update: venv
	@uv lock --upgrade
	@uv sync 

classify:
	@$(PYTHON) src/classify.py

phase-one:
	@$(PYTHON) src/run_sql.py sql/00_extensions_and_types.sql
	@$(PYTHON) src/run_sql.py sql/01_tables_pois.sql
	@$(PYTHON) src/run_sql.py sql/02_tables_admin_areas.sql
	@$(PYTHON) src/run_sql.py sql/04_tables_itineraries.sql
	@$(PYTHON) src/run_sql.py sql/05_tables_users.sql

phase-two:
	@$(PYTHON) src/load_themes.py
	@$(PYTHON) src/load_roles.py
	@$(PYTHON) src/load_pois.py
	@$(PYTHON) src/load_polygon.py

phase-three:
	@$(PYTHON) src/run_sql.py sql/20_link_admin_areas.sql
	@$(PYTHON) src/run_sql.py sql/31_function_poi_candidates.sql
	@$(PYTHON) src/run_sql.py sql/32_function_search_locations.sql
	@$(PYTHON) src/run_sql.py sql/33_function_search_pois.sql
	@$(PYTHON) src/run_sql.py sql/34_function_itinerary.sql


.PHONY: all venv sync sync-prod update classify phase-one phase-two phase-three
