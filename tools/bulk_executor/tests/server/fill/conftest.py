"""Conftest for fill/ generator tests.

The parent tests/server/conftest.py already mocks awsglue/pyspark/shared.
This nested conftest additionally injects a stub `faker` module so that
multi_entity_relationship.py (which does `from faker import Faker` at
module import time) can be collected without the real `faker` package
installed in the test environment.

The Faker stub returns deterministic-looking but cheap values so that
tests can assert on shape without depending on real faker output.
"""

import sys
import types
from unittest.mock import MagicMock


def _make_fake_faker():
    """Build a stub Faker class whose instances return predictable strings.

    Tests that need to assert specific values can replace individual methods
    on the `fake` module-level instance via monkeypatch.
    """
    class _StubFaker:
        def sentence(self, nb_words=3, variable_nb_words=True):
            return f"stub sentence {nb_words}"

        def paragraph(self):
            return "stub paragraph"

        def city(self):
            return "Stubville"

        def email(self):
            return "stub@example.test"

        def country(self):
            return "Stubland"

        def user_name(self):
            return "stubuser"

        def first_name(self):
            return "Stub"

        def last_name(self):
            return "User"

        def phone_number(self):
            return "+1-555-0100"

        def state(self):
            return "Stub State"

        def postcode(self):
            return "00000"

        def company(self):
            return "Stub, Inc."

        def domain_name(self):
            return "stub.test"

    return _StubFaker


# Only inject the stub if real `faker` isn't already importable
if 'faker' not in sys.modules:
    try:
        import faker  # noqa: F401
    except ImportError:
        _faker_module = types.ModuleType('faker')
        _faker_module.Faker = _make_fake_faker()
        sys.modules['faker'] = _faker_module
