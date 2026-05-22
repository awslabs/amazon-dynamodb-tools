# Testing
Ensure all integration tests and unit tests are placed in the `tests` folder. This ensures that no non-production code is pushed to Glue when we bootstrap the solution.

## Client testing

### Setup
1. Activate the python virtual environment `source .venv/bin/activate`
2. Install top-level python requirements `pip install -r requirements.txt`
3. Move to the client test directory `cd tests/client`
4. Running tests (Bash only)
   1. Run all tests `./run_tests.sh -v --tb=short`
   2. Run specific test for example `./run_tests.sh test_validate_s3_path.py -v`

## Server testing
Note that this is testing the server side code and does not use any AWS resources. Fake AWS credentials are set automatically by `tests/server/conftest.py` so no shell exports are needed.

### Setup
1. Activate the python virtual environment `source .venv/bin/activate`
2. Install top-level python requirements `pip install -r requirements.txt`
3. Install the server test requirements `pip install -r tests/server/load_export/requirements-test.txt`

### Running tests
Run from `tools/bulk_executor/`:

1. Run all server tests `python3 -m pytest tests/server`
2. Run only diff tests `python3 -m pytest tests/server/diff`
3. Run only load_export tests `python3 -m pytest tests/server/load_export`
4. Run a specific test file `python3 -m pytest tests/server/load_export/test_s3_validator.py -v`

> Heads up: an earlier `tests/server/run_tests.sh` wrapper was removed once the `python3 -m pytest` invocation became sufficient on its own. The wrapper used to bootstrap `awsglue`/`pyspark` mocks before pytest collection — that bootstrap now lives in `tests/server/conftest.py`, along with the fake AWS env vars.

### Coverage
Append the `--cov` flags to any of the commands above to print line coverage for `python_modules`:

1. All tests with coverage `python3 -m pytest tests/server --cov=python_modules --cov-report=term-missing`
2. Only diff tests with coverage `python3 -m pytest tests/server/diff --cov=python_modules --cov-report=term-missing`
