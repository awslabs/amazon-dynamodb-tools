# Bulk Import Capability

This utility allows you to do an import of DynamoDB table exported to S3 by leveraging Glue. The code is modularized to enable unit testing of individual components. It also leverages the `RateLimiter` classes to ensure that any bulk action executed on a table only consumes the capacity configured.

## Execution
Refer to the top level [README](../../../../../README.md) file

## Role requirements
The bulk import reads data from S3 and writes to an existing DynamoDB table, therefore it needs the following permissions:
1. Access to the S3 bucket in which the source DynamoDB export lives
2. Write access to the DynamoDB table to which the export needs to be restored to
3. If the DynamoDB table uses KMS keys, ensure the role has relevant access

## Unit testing
Refer to [README](../../../../tests/README.md)
1. Activate/create a python virtual environment using `source .venv/bin/activate` at the top level directory `/amazon-dynamodb-tools/bulk_executor`
2. Install top-level python requirements `pip install -r requirements.txt` 
3. Install the _Import_ module's test requirements `pip install -r server/src/python_modules/ddb_import/requirements-test.txt`
4. Move to folder `cd server/src/python_modules/ddb_import`
5. Because the way the boto3 imports are done at a module level, the tests for `ddb_import/writers` require fake AWS credentials, therefore run the tests using:
   1. Run a single test using `./run_tests.sh tests/test_s3_validator.py -v`
   2. Or run all unit tests `./run_tests.sh -v --tb=short`

## Benchmarking
1. 714230 items, G.1X, 220 DPU, WCUP/WCU 500/250, 48m51s
2. 714230 items, G.4X, 400 DPU, WCUP/WCU 500/400, 30m54s

Bottleneck:
   - could be the WCU
   - Given this is an IO bound operation (reading S3, writing to DDB), leverage fewer but larger workers, e.g. 50 G.4X workers instead of 220 G.1X
