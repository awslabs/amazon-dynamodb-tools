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

## Benchmarking
1. 714230 items, G.1X, 220 DPU, WCUP/WCU 500/250, 48m51s
2. 714230 items, G.4X, 400 DPU, WCUP/WCU 500/400, 30m54s

Bottleneck:
   - could be the WCU
   - Given this is an IO bound operation (reading S3, writing to DDB), leverage fewer but larger workers, e.g. 50 G.4X workers instead of 220 G.1X
