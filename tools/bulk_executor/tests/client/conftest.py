"""Shared fixtures for the client-side tests.

The client resolves its AWS region from the environment (`AWS_REGION`, then
`AWS_DEFAULT_REGION`, then the config file — matching the AWS CLI), which means a
developer's own exported region leaks into any test that exercises that resolution.
It did: after `EnvConfigs` started honouring `AWS_REGION`, six tests failed on a machine
with `AWS_REGION=us-west-2` exported and passed everywhere else. Scrub both variables for
every client test; a test that cares sets them itself with monkeypatch.
"""

import pytest

_REGION_ENV_VARS = ('AWS_REGION', 'AWS_DEFAULT_REGION')


@pytest.fixture(autouse=True)
def isolate_aws_region_env(monkeypatch):
    for name in _REGION_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
