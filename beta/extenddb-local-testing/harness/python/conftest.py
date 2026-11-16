"""Gives the suite a DynamoDB endpoint, however the environment supplies one.

In CI with a service container, DDB_ENDPOINT is already set and these fixtures
just use it. On a laptop with nothing running, the session fixture starts the
ExtendDB dev container and stops it afterwards. There is no Python launcher
package, so Python uses the container.
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
from botocore.exceptions import ClientError, HTTPClientError
from botocore.exceptions import ConnectionError as BotoConnectionError

IMAGE = "extenddb/extenddb-dev:0.1.14"
# AWS's published example credentials. They are public and safe to commit.
EXAMPLE_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
EXAMPLE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
REGION = "us-east-1"
AUTH_ERRORS = ("UnrecognizedClientException", "InvalidSignatureException")


def credentials():
    """The (key id, secret) pair the suite signs with."""
    key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    # Only use the environment pair if both halves are set.
    if key_id and secret:
        return key_id, secret
    return EXAMPLE_KEY_ID, EXAMPLE_SECRET


def table_name(suffix):
    """A table name unique to this call, so parallel shards sharing one server
    get separate tables."""
    return f"extenddb-ci-{suffix}-{os.getpid()}-{uuid.uuid4().hex[:12]}"


def eventually(fn, timeout=5.0, interval=0.025):
    """Retry an assertion until it stops raising.

    GSI reads can lag writes, on ExtendDB and on real DynamoDB. Poll instead
    of sleeping.
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
    # A skipped session exits 0, so fail instead.
    pytest.fail(
        "No DynamoDB endpoint available: DDB_ENDPOINT is unset and neither docker "
        "nor podman is installed. Either set DDB_ENDPOINT to a running ExtendDB "
        "(or DynamoDB) server, or install a container runtime so the suite can "
        "start one.",
        pytrace=False,
    )


def _wait_ready(url, timeout=60.0):
    """Wait until the server at url answers a request.

    Any reply counts, including an error. Only connection-level failures are
    retried. A credentials error stops the run at once with a clear message.
    """
    key_id, secret = credentials()
    client = boto3.client(
        "dynamodb", endpoint_url=url, region_name=REGION,
        aws_access_key_id=key_id, aws_secret_access_key=secret,
        config=Config(retries={"max_attempts": 1, "mode": "standard"}),
    )
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            client.list_tables()
            return
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            break
        except (BotoConnectionError, HTTPClientError) as error:
            last = error
            time.sleep(0.1)
    else:
        raise RuntimeError(
            f"ExtendDB at {url} did not become ready in {timeout}s; last error: {last!r}"
        ) from last
    # The server answered with an error, so it is up.
    if code in AUTH_ERRORS:
        pytest.fail(
            f"ExtendDB at {url} rejected the credentials ({code}) for key id "
            f"{key_id}. ExtendDB verifies SigV4 and seeds only the example pair "
            f"{EXAMPLE_KEY_ID}. Unset AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, "
            "or export the example pair.",
            pytrace=False,
        )


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
        try:
            subprocess.run(
                [runtime, "run", "-d", "--name", name,
                 "-p", f"127.0.0.1:{port}:18080",
                 "-e", "EXTENDDB__STORAGE__SQLITE__PATH=:memory:", IMAGE],
                check=True, capture_output=True, text=True,
            )
        except subprocess.CalledProcessError as error:
            raise RuntimeError(
                f"`{runtime} run` failed (exit {error.returncode}): {error.stderr.strip()}"
            ) from error
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
    write issued immediately fails with ResourceNotFoundException. boto3's
    table_exists waiter handles that, against DynamoDB Local and real DynamoDB
    too.
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
        # boto3 inherits the DynamoDB waiter spec's default of 20 s between
        # polls, which would dominate a local run. A local table leaves
        # CREATING in well under a second, so poll every second.
        db.get_waiter("table_exists").wait(
            TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30}
        )
    except Exception:
        # Clean up best-effort, and report the waiter's error, not a cleanup one.
        try:
            db.delete_table(TableName=name)
        except Exception:
            pass
        raise
    yield name
    db.delete_table(TableName=name)
