"""The shared suite, in Python. Mirrors the TypeScript suite's assertions."""
import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

# pytest puts this directory on sys.path (rootdir-based "prepend" import mode),
# so conftest.py imports as a plain module.
from conftest import REGION, credentials, eventually, table_name


def item(n, status="NEW"):
    return {
        "pk": {"S": f"CUST#{n}"},
        "sk": {"S": f"ORDER#{n}"},
        "status": {"S": status},
        "createdAt": {"S": f"2026-01-01T00:00:{n:02d}Z"},
        "amount": {"N": str(n * 10)},
    }


def key(n):
    return {"pk": {"S": f"CUST#{n}"}, "sk": {"S": f"ORDER#{n}"}}


def test_writes_and_reads_an_item(db, table):
    db.put_item(TableName=table, Item=item(1))
    res = db.get_item(TableName=table, Key=key(1), ConsistentRead=True)
    assert res["Item"]["status"]["S"] == "NEW"


def test_rejects_a_write_whose_condition_fails(db, table):
    db.put_item(TableName=table, Item=item(2))
    with pytest.raises(ClientError) as exc:
        db.put_item(
            TableName=table,
            Item=item(2),
            ConditionExpression="attribute_not_exists(pk)",
        )
    assert exc.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


def test_writes_a_batch(db, table):
    db.batch_write_item(RequestItems={
        table: [{"PutRequest": {"Item": item(n)}} for n in (3, 4, 5)]
    })
    for n in (3, 4, 5):
        res = db.get_item(TableName=table, Key=key(n), ConsistentRead=True)
        assert "Item" in res


def test_transaction_commits_both_members(db, table):
    db.transact_write_items(TransactItems=[
        {"Put": {"TableName": table, "Item": item(10, "PAID")}},
        {"Put": {"TableName": table, "Item": item(11, "PAID")}},
    ])
    res = db.get_item(TableName=table, Key=key(10), ConsistentRead=True)
    assert res["Item"]["status"]["S"] == "PAID"


def test_transaction_cancels_when_one_condition_fails(db, table):
    with pytest.raises(ClientError) as exc:
        db.transact_write_items(TransactItems=[
            {"Put": {"TableName": table, "Item": item(12)}},
            {"Put": {
                "TableName": table,
                "Item": item(13),
                "ConditionExpression": "attribute_exists(pk)",
            }},
        ])
    assert exc.value.response["Error"]["Code"] == "TransactionCanceledException"
    res = db.get_item(TableName=table, Key=key(12), ConsistentRead=True)
    assert "Item" not in res


def test_queries_the_base_table(db, table):
    db.put_item(TableName=table, Item=item(20))
    res = db.query(
        TableName=table,
        KeyConditionExpression="pk = :p AND begins_with(sk, :s)",
        ExpressionAttributeValues={":p": {"S": "CUST#20"}, ":s": {"S": "ORDER#"}},
    )
    assert res["Count"] == 1


def test_queries_a_local_secondary_index(db, table):
    db.put_item(TableName=table, Item=item(30))
    # A local secondary index is strongly consistent: no retry needed.
    res = db.query(
        TableName=table,
        IndexName="byAmount",
        KeyConditionExpression="pk = :p AND #a > :a",
        ExpressionAttributeNames={"#a": "amount"},
        ExpressionAttributeValues={":p": {"S": "CUST#30"}, ":a": {"N": "0"}},
    )
    assert res["Count"] == 1


# Unlike a local index, a GSI read right after a write is eventually
# consistent: it lags by a few milliseconds on ExtendDB by default and is
# always allowed to on real DynamoDB. `eventually` is the portable answer.
def test_queries_a_global_secondary_index_once_caught_up(db, table):
    db.put_item(TableName=table, Item=item(40, "SHIPPED"))

    def read_index():
        res = db.query(
            TableName=table,
            IndexName="byStatus",
            KeyConditionExpression="#s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": {"S": "SHIPPED"}},
        )
        assert res["Count"] == 1
        return res

    eventually(read_index)


def test_enables_ttl(db, table):
    res = db.update_time_to_live(
        TableName=table,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expiresAt"},
    )
    assert res["TimeToLiveSpecification"]["AttributeName"] == "expiresAt"


def test_rejects_a_request_signed_with_the_wrong_secret(endpoint):
    # ExtendDB verifies SigV4; DynamoDB Local accepts any credentials. A suite
    # ported from DynamoDB Local with mismatched dummy keys fails here. The key
    # id is a known one, so the server reaches signature checking and reports
    # InvalidSignatureException (an unknown key id would give
    # UnrecognizedClientException instead).
    key_id, _ = credentials()
    bad = boto3.client(
        "dynamodb",
        endpoint_url=endpoint,
        region_name=REGION,
        aws_access_key_id=key_id,
        aws_secret_access_key="wrong",
        config=Config(retries={"max_attempts": 1, "mode": "standard"}),
    )
    with pytest.raises(ClientError) as exc:
        bad.list_tables()
    assert exc.value.response["Error"]["Code"] == "InvalidSignatureException"


def test_table_names_are_distinct_per_call_even_for_the_same_suffix():
    assert table_name("alpha") != table_name("alpha")
    assert table_name("alpha").startswith("extenddb-ci-alpha-")
