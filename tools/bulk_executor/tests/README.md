# Testing

Ensure all integration tests and unit tests are placed in the `tests` folder. This ensures that no non-production code is pushed to Glue when we bootstrap the solution.

## Quick start

From `tools/bulk_executor/`:

```
make install     # one-time: create .venv and install dependencies
make test        # run all tests (client + server)
```

That's the whole flow. No `source .venv/bin/activate` to remember, no `PYTHONPATH=` exports, no AWS env vars.

## Other targets

```
make test-client     # client tests only
make test-server     # server tests only
make coverage        # tests with coverage report
make clean           # remove .venv and caches
make help            # list available targets
```

## Without make (power users)

If you'd rather drive pytest directly, activate the venv and call it:

```
source .venv/bin/activate
pytest                                                       # all tests
pytest tests/client                                          # client only
pytest tests/server                                          # all server tests
pytest tests/server/diff                                     # diff tests only
pytest tests/server/shared/export                            # shared code for *_export functionality tests only
pytest tests/server/load_export                              # load_export tests only
pytest tests/server/revert_export                            # revert_export tests only
pytest tests/client/diff/test_validate_s3_path.py -v         # a single file
pytest --cov=server/src --cov=client/src --cov-branch --cov-report=term-missing
```

Test configuration lives in `pytest.ini` at the project root. It sets `pythonpath` to cover both `client/src` and `server/src`, so any subset of tests can be invoked from `tools/bulk_executor/`. The mock bootstrap for `awsglue` and `pyspark` (used by the server-side tests) lives in `tests/server/conftest.py` and runs automatically at pytest collection time.
