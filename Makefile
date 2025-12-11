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

clean:
	@rm -rf build/ src/*.egg-info/ cache/

phase-one:
	@$(PYTHON) src/run_sql.py sql/00_extensions_and_types.sql
	@$(PYTHON) src/run_sql.py sql/01_tables_pois.sql
# 	@$(PYTHON) src/run_sql.py sql/02_tables_admin_areas.sql

phase-two:
	@$(PYTHON) src/load_themes.py
	@$(PYTHON) src/load_roles.py
	@$(PYTHON) src/load_pois.py
# 	@$(PYTHON) src/load_polygon.py

phase-three:
# 	@$(PYTHON) src/run_sql.py sql/20_link_admin_areas.sql
# 	@$(PYTHON) src/run_sql.py sql/31_function_poi_candidates.sql
# 	@$(PYTHON) src/run_sql.py sql/32_function_search_locations.sql
	@$(PYTHON) src/run_sql.py sql/33_function_search_pois.sql

copy-webdata:
	@cp ../../gmap-scraper/webdata/* data/map/

copy-michelin:
	@cp ../../michelin-my-maps/data/michelin*.csv output/

.PHONY: all venv sync sync-prod update classify clean copy-webdata
