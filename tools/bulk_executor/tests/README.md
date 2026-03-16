# Testing
Ensure all integration tests and unit tests are placed in the `tests` folder. This ensures that no non-production code is pushed to Glue when we bootstrap the solution.

## Client testing
TODO

## Server testing

### DynamoDB Import Unit Testing

#### Setup
1. Activate the python virtual environment `source .venv/bin/activate`
2. Install top-level python requirements `pip install -r requirements.txt`
3. Move to the DynamoDB Import (ddb_import) directory `cd tests/server/ddb_import`
4. Install the __ module's test requirements `pip install -r server/src/python_modules/ddb_import/requirements-test.txt`
5. Running tests
   1. Run all tests `./run_tests.sh -v --tb=short`
   2. Run specific test for example `./run_tests.sh test_s3_validator.py -v`