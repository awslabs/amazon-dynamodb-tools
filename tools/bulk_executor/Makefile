.DEFAULT_GOAL := help
.PHONY: help install test test-client test-server coverage clean

VENV := .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest

# Source uses PEP 604 union syntax (X | None), which requires Python 3.10+.
# Prefer python3.11, fall back through 3.10, then plain python3.
PYTHON := $(shell command -v python3.11 || command -v python3.10 || command -v python3)

help:
	@echo "Targets:"
	@echo "  make install      Create venv and install dependencies (run this first)"
	@echo "  make test         Run all tests (client + server)"
	@echo "  make test-client  Run client tests only"
	@echo "  make test-server  Run server tests only"
	@echo "  make coverage     Run tests with coverage report"
	@echo "  make clean        Remove venv and caches"

install:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install -r requirements.txt
	$(PIP) install -r tests/requirements-test.txt

test:
	@$(PYTEST) -q --tb=short --cov=server/src --cov=client/src --cov-branch --cov-report= 2>&1 | tail -8
	@$(VENV)/bin/coverage json -o .coverage.json --quiet 2>/dev/null && \
	  $(VENV)/bin/python -c "import json; t=json.load(open('.coverage.json'))['totals']; \
	    print(); \
	    print(f\"  LINE COVERAGE:   {t['percent_statements_covered']:5.1f}%  ({t['covered_lines']}/{t['num_statements']} statements covered)\"); \
	    print(f\"  BRANCH COVERAGE: {t['percent_branches_covered']:5.1f}%  ({t['covered_branches']}/{t['num_branches']} branches covered)\"); \
	    print()" && rm -f .coverage.json

test-client:
	$(PYTEST) tests/client -q --tb=short

test-server:
	$(PYTEST) tests/server -q --tb=short

coverage:
	$(PYTEST) --cov=server/src --cov=client/src --cov-branch --cov-report=term-missing

clean:
	rm -rf $(VENV) .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name .coverage -delete
