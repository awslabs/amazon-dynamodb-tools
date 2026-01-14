# ddbtools - ARCHIVED

**Status:** Deprecated and Archived  
**Date Archived:** January 2026  
**Reason:** Only used by archived tools

## What was ddbtools?

A Python utility library that provided helper functions for DynamoDB operations. It included:
- `TableUtility` - DynamoDB table operations (describe, list, tagging)
- `PricingUtility` - Cost calculations
- `DecimalEncoder` - JSON serialization for DynamoDB Decimal types
- Constants and utilities

## Why was it archived?

This library was only used by `table_tagger.py` (see `archived/table_tagger/`), which has also been archived. With no other consumers, there's no reason to maintain it.

## Need similar functionality?

Use `boto3` (the official AWS SDK for Python) directly - it provides comprehensive DynamoDB APIs with better support and documentation.

## Related

- `archived/table_tagger/` - The primary (and only active) consumer of this library
- [#114](https://github.com/awslabs/amazon-dynamodb-tools/issues/114) - Archive deprecated utilities
