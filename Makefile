VENV = .venv
PYTHON = python3

all: venv install

venv: $(VENV)/bin/activate

$(VENV)/bin/activate: pyproject.toml
	$(PYTHON) -m venv $(VENV)
	@echo "Virtual environment created."

install: venv
	@.venv/bin/pip install --upgrade pip setuptools wheel
	@.venv/bin/pip install .
	@echo "Dependencies installed from pyproject.toml."

upgrade:
	@.venv/bin/pip install --upgrade pip setuptools wheel
	@.venv/bin/pip install --upgrade .

clean:
	rm -rf __pycache__/ src/cache/ cache/

.PHONY: all venv install upgrade clean
