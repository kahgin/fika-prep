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
	@python src/upload_pois_to_supabase.py

.PHONY: all venv sync sync-prod update clean cleandist upload
