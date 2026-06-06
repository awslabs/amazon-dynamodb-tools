# Bulk Revert-Export Capability

This command reverts changes captured in an incremental DynamoDB table export by swapping `new_image` with `old_image` before writing. It shares the same pipeline as [load-export](../load_export/README.md) (validation, reading, transforms, writing, rate limiting) but natively applies the revert logic without requiring a user-specified transform.

## Execution
Refer to the top level [README](../../../../../README.md) file

## Role requirements
Same as [load-export](../load_export/README.md#role-requirements).

## Unit testing
Refer to [README](../../../../tests/README.md)
