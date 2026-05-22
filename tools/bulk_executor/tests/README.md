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
Note that this is testing the server side code and does not use any AWS resources. The bootstrap that mocks `awsglue` and `pyspark` lives in `tests/server/load_export/conftest.py` and runs automatically at pytest collection time — no shell wrapper needed.

### DynamoDB Load-Export Unit Testing

#### Setup
1. Activate the python virtual environment `source .venv/bin/activate`
2. Install top-level python requirements `pip install -r requirements.txt`
3. Install the module's test requirements `pip install -r tests/server/load_export/requirements-test.txt`

#### Running tests
From `tools/bulk_executor/`:
1. Run all load_export tests `python3 -m pytest tests/server/load_export -v --tb=short`
2. Run a specific file `python3 -m pytest tests/server/load_export/test_s3_validator.py -v`

#### Coverage
Append `pytest-cov` flags to any of the commands above:
1. All load_export tests with coverage `python3 -m pytest tests/server/load_export --cov=python_modules.load_export --cov-report=term-missing`
2. Single file with coverage `python3 -m pytest tests/server/load_export/test_s3_validator.py --cov=python_modules.load_export --cov-report=term-missing`