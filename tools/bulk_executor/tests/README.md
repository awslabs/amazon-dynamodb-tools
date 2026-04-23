# Testing
Ensure all integration tests and unit tests are placed in the `tests` folder. This ensures that no non-production code is pushed to Glue when we bootstrap the solution.

## Client testing

### Setup
1. Activate the python virtual environment `source .venv/bin/activate`
2. Install top-level python requirements `pip install -r requirements.txt`
3. Move to the client test directory `cd tests/client`
4. Running tests
   1. Run all tests `./run_tests.sh -v --tb=short`
   2. Run specific test for example `./run_tests.sh test_validate_s3_path.py -v`

## Server testing

### DynamoDB Load-Export Unit Testing

#### Setup
1. Activate the python virtual environment `source .venv/bin/activate`
2. Install top-level python requirements `pip install -r requirements.txt`
3. Move to the DynamoDB Load-Export (load_export) directory `cd tests/server/load_export`
4. Install the module's test requirements `pip install -r requirements-test.txt`
5. Running tests
   1. Run all tests `./run_tests.sh -v --tb=short`
   2. Run specific test for example `./run_tests.sh test_s3_validator.py -v`