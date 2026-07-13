.DEFAULT_GOAL := help
.PHONY: help install test coverage lint clean

VENV := .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest

PYTHON := $(shell command -v python3.11 || command -v python3.10 || command -v python3)

help:
	@echo "Targets:"
	@echo "  make install   Create venv and install dev dependencies"
	@echo "  make test      Run unit tests with coverage summary"
	@echo "  make coverage  Run tests with detailed missing-line coverage"
	@echo "  make lint      Syntax check shell + python sources"
	@echo "  make clean     Remove venv and caches"

install:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements-dev.txt

test:
	@$(PYTEST) -q --tb=short --cov=scripts --cov=lambda --cov-branch --cov-report= 2>&1 | tail -10
	@$(VENV)/bin/coverage json -o .coverage.json --quiet 2>/dev/null && \
	  $(VENV)/bin/python -c "import json; t=json.load(open('.coverage.json'))['totals']; \
	    print(); \
	    print(f\"  LINE COVERAGE:   {t['percent_statements_covered']:5.1f}%  ({t['covered_lines']}/{t['num_statements']} statements covered)\"); \
	    print(f\"  BRANCH COVERAGE: {t['percent_branches_covered']:5.1f}%  ({t['covered_branches']}/{t['num_branches']} branches covered)\"); \
	    print()" && rm -f .coverage.json

coverage:
	$(PYTEST) --cov=scripts --cov=lambda --cov-branch --cov-report=term-missing

lint:
	@for f in deploy.sh teardown.sh demo/run_demo.sh; do bash -n $$f && echo "  OK $$f" || exit 1; done
	@find . -name '*.py' -not -path './.venv/*' -exec $(VENV)/bin/python -m py_compile {} +
	@echo "  OK python syntax"

clean:
	rm -rf $(VENV) .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name .coverage -delete
	find . -type f -name .coverage.json -delete
