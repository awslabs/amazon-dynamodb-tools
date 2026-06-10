.DEFAULT_GOAL := help
.PHONY: help install test test-client test-server coverage clean test-e2e test-e2e-connector test-e2e-cleanup test-e2e-security test-e2e-security-simulator test-e2e-security-real test-e2e-commands

VENV := .venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest

# Source uses PEP 604 union syntax (X | None), which requires Python 3.10+.
# Prefer python3.11, fall back through 3.10, then plain python3.
PYTHON := $(shell command -v python3.11 || command -v python3.10 || command -v python3)

help:
	@echo "Unit tests (offline, free, fast):"
	@echo "  make install              Create venv and install dependencies (run this first)"
	@echo "  make test                 Run all unit tests (client + server) with coverage"
	@echo "  make test-client          Run client unit tests only"
	@echo "  make test-server          Run server unit tests only"
	@echo "  make coverage             Unit tests with term-missing coverage report"
	@echo "  make clean                Remove venv and caches"
	@echo ""
	@echo "End-to-end tests (real AWS, costs money, ~10-15 min per suite):"
	@echo "  make test-e2e                       List available e2e suites"
	@echo "  make test-e2e-connector             Connector smoke: count/find/sql/load via dataframe"
	@echo "  make test-e2e-security              Security: simulator + real-bootstrap (~3 min, ~\$$0)"
	@echo "  make test-e2e-security-simulator    Security tier 1 only: iam:SimulateCustomPolicy"
	@echo "  make test-e2e-security-real         Security tier 2 only: real temp IAM user + bootstrap"
	@echo "  make test-e2e-commands              Bulk-command smokes against transient tables"
	@echo "  make test-e2e-cleanup               Delete orphaned items left by a crashed e2e run"

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

# --- e2e targets -------------------------------------------------------------
# Each e2e suite is opt-in; 'make test' never runs them. They prompt for AWS
# config on first run and persist answers to tests/e2e/.e2e-config (gitignored).

test-e2e:
	@echo "Available e2e suites:"
	@echo "  make test-e2e-connector   Connector smoke: count/find/sql/load via dataframe"
	@echo "  make test-e2e-security    IAM doc validation: simulator + real bootstrap"
	@echo "  make test-e2e-commands    Bulk-command smokes (fill/update/delete/copy/diff)"
	@echo ""
	@echo "First run prompts for account, region, and test tables."
	@echo "Subsequent runs use tests/e2e/.e2e-config (delete to re-prompt)."

test-e2e-connector:
	$(PYTEST) tests/e2e/connector -s -v --tb=short --no-cov \
	  --override-ini="testpaths=tests/e2e" \
	  --e2e-suite="connector smoke"

# Security suite: simulator runs first so the real IAM-creating tier
# auto-skips if the documented policy already fails simulation.
test-e2e-security:
	$(PYTEST) tests/e2e/security -s -v --tb=short --no-cov \
	  --override-ini="testpaths=tests/e2e" \
	  --e2e-suite="security (IAM doc validation)"

test-e2e-security-simulator:
	$(PYTEST) tests/e2e/security/test_simulator.py -s -v --tb=short --no-cov \
	  --override-ini="testpaths=tests/e2e" \
	  --e2e-suite="security simulator"

test-e2e-security-real:
	$(PYTEST) tests/e2e/security/test_real_bootstrap.py -s -v --tb=short --no-cov \
	  --override-ini="testpaths=tests/e2e" \
	  --e2e-suite="security real-bootstrap"

# Commands suite: exercises bulk commands (fill/update/delete/copy/diff)
# against transient tables created+torn-down per test. No pre-existing
# tables required.
test-e2e-commands:
	$(PYTEST) tests/e2e/commands -s -v --tb=short --no-cov \
	  --override-ini="testpaths=tests/e2e" \
	  --e2e-suite="bulk commands"

test-e2e-cleanup:
	$(VENV)/bin/python -m tests.e2e.helpers.cleanup
