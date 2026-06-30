"""Unit tests for client/src/infrastructure/constants.py.

Verifies that third-party dependency declarations are correct:
- faker MUST be available for the 'fill' verb (moved from global to verb-specific)
- Non-fill verbs must not pull in faker (the performance optimization)
"""

from infrastructure.constants import (
    _THIRD_PARTY_PYTHON_MODULES,
    THIRD_PARTY_PYTHON_MODULES,
    VERB_PYTHON_MODULES,
)


class TestVerbPythonModules:
    """Verify verb-specific dependency declarations."""

    def test_faker_declared_for_fill_verb(self):
        """faker must be available for the fill verb — it generates fake data."""
        assert 'fill' in VERB_PYTHON_MODULES
        assert 'faker' in VERB_PYTHON_MODULES['fill']

    def test_faker_not_in_global_modules(self):
        """faker should NOT be in global modules (perf: avoid pip install on every run)."""
        assert 'faker' not in _THIRD_PARTY_PYTHON_MODULES
        assert 'faker' not in THIRD_PARTY_PYTHON_MODULES

    def test_non_fill_verbs_do_not_include_faker(self):
        """Verbs other than fill must not declare faker as a dependency."""
        for verb, modules in VERB_PYTHON_MODULES.items():
            if verb != 'fill':
                assert 'faker' not in modules, f"verb '{verb}' should not require faker"
