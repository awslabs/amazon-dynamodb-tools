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
Note that this is testing the server side code and does not use any AWS resources.

### Setup
1. Activate the python virtual environment `source .venv/bin/activate`
2. Install top-level python requirements `pip install -r requirements.txt`
3. Move to the server tests directory `cd tests/server`
4. Install the module's test requirements `pip install -r load_export/requirements-test.txt`

### Running tests (Bash only)
1. Run all server tests (diff + load_export) `./run_tests.sh -v --tb=short`
2. Run only diff tests `./run_tests.sh diff -v --tb=short`
3. Run only load_export tests `./run_tests.sh load_export -v --tb=short`
4. Run specific test file for example `./run_tests.sh load_export/test_s3_validator.py -v`