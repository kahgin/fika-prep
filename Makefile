VENV = .venv

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

clean:
	@rm -rf build/ src/*.egg-info/ cache/

upload:
	@python src/load_themes.py
	@python src/load_roles.py
	@python src/load_pois.py
	@python src/load_polygon.py

.PHONY: all venv sync sync-prod update clean upload
