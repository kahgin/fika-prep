VENV = .venv
PYTHON = python3

$(VENV)/bin/activate: pyproject.toml
	$(PYTHON) -m venv $(VENV)
	@echo "Virtual environment created."

all: install

venv: $(VENV)/bin/activate

install: venv
	@$(VENV)/bin/pip install --upgrade pip setuptools wheel
	@$(VENV)/bin/pip install .

upgrade:
	@$(VENV)/bin/pip install --upgrade pip setuptools wheel
	@$(VENV)/bin/pip install --upgrade .

clean:
	rm -rf build/ src/*.egg-info/ 

.PHONY: all venv install upgrade clean
