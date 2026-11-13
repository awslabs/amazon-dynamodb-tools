"""Gives the suite a DynamoDB endpoint, however the environment supplies one.

In CI with a service container, DDB_ENDPOINT is already set and these fixtures
just use it. On a laptop with nothing running, the session fixture starts the
ExtendDB dev container and stops it afterwards. There is no Python launcher
package, so the container is the portable answer for Python.
"""
import os
import shutil
import socket
import subprocess
import time
import uuid

import boto3
import pytest
from botocore.config import Config

IMAGE = "extenddb/extenddb-dev:0.1.14"
# The AWS documentation's example credentials: public, and safe to commit.
EXAMPLE_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
EXAMPLE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
REGION = "us-east-1"


def credentials():
    """The (key id, secret) pair the suite signs with."""
    return (
        os.environ.get("AWS_ACCESS_KEY_ID", EXAMPLE_KEY_ID),
        os.environ.get("AWS_SECRET_ACCESS_KEY", EXAMPLE_SECRET),
    )


def table_name(suffix):
    """A table name unique to this caller and run, so parallel shards sharing
    one server never collide. The same habit works against real DynamoDB."""
    return f"extenddb-ci-{suffix}-{os.getpid()}-{uuid.uuid4().hex[:12]}"


def eventually(fn, timeout=5.0, interval=0.025):
    """Retry an assertion until it stops raising.

    Secondary index reads lag their writes: on ExtendDB by its default
    propagation delay, on real DynamoDB always. Polling is the portable way to
    assert on an index without a sleep that is either flaky or slow.
    """
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            return fn()
        except AssertionError as error:
            last = error
            time.sleep(interval)
    raise last if last else AssertionError("eventually: timed out before first attempt")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _container_runtime():
    for candidate in ("docker", "podman"):
        if shutil.which(candidate):
            return candidate
    pytest.skip("neither docker nor podman is installed, and DDB_ENDPOINT is unset")


def _wait_ready(url, timeout=60.0):
    key_id, secret = credentials()
    client = boto3.client(
        "dynamodb", endpoint_url=url, region_name=REGION,
        aws_access_key_id=key_id, aws_secret_access_key=secret,
        config=Config(retries={"max_attempts": 1, "mode": "standard"}),
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            client.list_tables()
            return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"ExtendDB at {url} did not become ready in {timeout}s")


@pytest.fixture(scope="session")
def endpoint():
    """The endpoint the suite talks to; starts a container only if needed."""
    supplied = os.environ.get("DDB_ENDPOINT")
    if supplied:
        _wait_ready(supplied)
        yield supplied
        return

    runtime = _container_runtime()
    port = _free_port()
    name = f"extenddb-pytest-{uuid.uuid4().hex[:8]}"
    try:
        subprocess.run(
            [runtime, "run", "-d", "--name", name,
             "-p", f"127.0.0.1:{port}:18080",
             "-e", "EXTENDDB__STORAGE__SQLITE__PATH=:memory:", IMAGE],
            check=True, capture_output=True,
        )
        url = f"http://127.0.0.1:{port}"
        _wait_ready(url)
        yield url
    finally:
        subprocess.run([runtime, "rm", "-f", name], check=False, capture_output=True)


@pytest.fixture(scope="session")
def db(endpoint):
    key_id, secret = credentials()
    return boto3.client(
        "dynamodb",
        endpoint_url=endpoint,
        region_name=REGION,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
    )


@pytest.fixture(scope="session")
def table(db):
    """Create the suite's table and wait until it is usable.

    ExtendDB returns CREATING from CreateTable, as the real service does, so a
    write issued immediately fails with ResourceNotFoundException. boto3's own
    waiter is the portable fix: this same code is correct against DynamoDB Local
    and against real DynamoDB.
    """
    name = table_name("python")
    db.create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
            {"AttributeName": "createdAt", "AttributeType": "S"},
            {"AttributeName": "amount", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "byStatus",
            "KeySchema": [
                {"AttributeName": "status", "KeyType": "HASH"},
                {"AttributeName": "createdAt", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
        LocalSecondaryIndexes=[{
            "IndexName": "byAmount",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "amount", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
    )
    try:
        # The DynamoDB waiter spec's default is 20 s between polls, which boto3
        # inherits and which would dominate a local run. These values keep the
        # wait proportional to a local server, where a table leaves CREATING in
        # about a second.
        db.get_waiter("table_exists").wait(
            TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30}
        )
    except Exception:
        # Setup failed: clean up best-effort, but let the waiter's own error
        # surface rather than a secondary one from the cleanup.
        try:
            db.delete_table(TableName=name)
        except Exception:
            pass
        raise
    yield name
    db.delete_table(TableName=name)
