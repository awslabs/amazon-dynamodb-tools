"""Resolve the S3 bucket created by `bulk bootstrap`.

The bootstrap step stashes the bucket name in the Glue job's
DefaultArguments under '--s3-bucket-name'. We read it from there so the
e2e suite doesn't need a separate config prompt for it.
"""
from __future__ import annotations

import boto3

GLUE_JOB_NAME = "bulk_dynamodb"


def discover_bucket(region: str) -> str:
    """Return the bootstrap-created S3 bucket for this account+region.

    Raises if the Glue job doesn't exist (developer skipped bootstrap)
    or if the DefaultArguments are missing the bucket key (corrupted
    bootstrap state).
    """
    glue = boto3.client("glue", region_name=region)
    response = glue.get_job(JobName=GLUE_JOB_NAME)
    args = response["Job"]["DefaultArguments"]
    bucket = args.get("--s3-bucket-name")
    if not bucket:
        raise RuntimeError(
            f"Glue job {GLUE_JOB_NAME!r} has no '--s3-bucket-name' default "
            f"argument. Re-run 'bulk bootstrap' in this account+region."
        )
    return bucket
