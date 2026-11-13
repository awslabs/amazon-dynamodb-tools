import { describe, it, expect, beforeAll, afterAll, inject } from "vitest";
import {
  DynamoDBClient, PutItemCommand, GetItemCommand, QueryCommand,
  BatchWriteItemCommand, TransactWriteItemsCommand, DeleteTableCommand,
  ListTablesCommand, UpdateTimeToLiveCommand,
} from "@aws-sdk/client-dynamodb";
import { client, tableName, createTable, eventually } from "./support.mjs";

const TABLE = tableName("dynamodb");
let db;

beforeAll(async () => {
  db = client();
  await createTable(db, TABLE);
});

afterAll(async () => {
  if (!db) return; // beforeAll failed; let its own error surface
  await db.send(new DeleteTableCommand({ TableName: TABLE }));
});

const item = (n, status = "NEW") => ({
  pk: { S: `CUST#${n}` },
  sk: { S: `ORDER#${n}` },
  status: { S: status },
  createdAt: { S: `2026-01-01T00:00:${String(n).padStart(2, "0")}Z` },
  amount: { N: String(n * 10) },
});

describe("item operations", () => {
  it("writes and reads an item", async () => {
    await db.send(new PutItemCommand({ TableName: TABLE, Item: item(1) }));
    const res = await db.send(new GetItemCommand({
      TableName: TABLE,
      Key: { pk: { S: "CUST#1" }, sk: { S: "ORDER#1" } },
      ConsistentRead: true,
    }));
    expect(res.Item?.status?.S).toBe("NEW");
  });

  it("rejects a write whose condition fails", async () => {
    await db.send(new PutItemCommand({ TableName: TABLE, Item: item(2) }));
    await expect(db.send(new PutItemCommand({
      TableName: TABLE, Item: item(2),
      ConditionExpression: "attribute_not_exists(pk)",
    }))).rejects.toMatchObject({ name: "ConditionalCheckFailedException" });
  });

  it("writes a batch", async () => {
    await db.send(new BatchWriteItemCommand({
      RequestItems: {
        [TABLE]: [3, 4, 5].map((n) => ({ PutRequest: { Item: item(n) } })),
      },
    }));
    for (const n of [3, 4, 5]) {
      const res = await db.send(new GetItemCommand({
        TableName: TABLE,
        Key: { pk: { S: `CUST#${n}` }, sk: { S: `ORDER#${n}` } },
        ConsistentRead: true,
      }));
      expect(res.Item).toBeDefined();
    }
  });
});

describe("transactions", () => {
  it("commits both members", async () => {
    await db.send(new TransactWriteItemsCommand({
      TransactItems: [
        { Put: { TableName: TABLE, Item: item(10, "PAID") } },
        { Put: { TableName: TABLE, Item: item(11, "PAID") } },
      ],
    }));
    const res = await db.send(new GetItemCommand({
      TableName: TABLE,
      Key: { pk: { S: "CUST#10" }, sk: { S: "ORDER#10" } },
      ConsistentRead: true,
    }));
    expect(res.Item?.status?.S).toBe("PAID");
  });

  it("cancels the whole transaction when one condition fails", async () => {
    await expect(db.send(new TransactWriteItemsCommand({
      TransactItems: [
        { Put: { TableName: TABLE, Item: item(12) } },
        {
          Put: {
            TableName: TABLE, Item: item(13),
            ConditionExpression: "attribute_exists(pk)",
          },
        },
      ],
    }))).rejects.toMatchObject({ name: "TransactionCanceledException" });

    const res = await db.send(new GetItemCommand({
      TableName: TABLE,
      Key: { pk: { S: "CUST#12" }, sk: { S: "ORDER#12" } },
      ConsistentRead: true,
    }));
    expect(res.Item).toBeUndefined();
  });
});

describe("queries", () => {
  it("queries the base table", async () => {
    await db.send(new PutItemCommand({ TableName: TABLE, Item: item(20) }));
    const res = await db.send(new QueryCommand({
      TableName: TABLE,
      KeyConditionExpression: "pk = :p AND begins_with(sk, :s)",
      ExpressionAttributeValues: { ":p": { S: "CUST#20" }, ":s": { S: "ORDER#" } },
    }));
    expect(res.Count).toBe(1);
  });

  it("queries a local secondary index", async () => {
    await db.send(new PutItemCommand({ TableName: TABLE, Item: item(30) }));
    // An LSI read can be strongly consistent when asked for, so this test asks
    // (ConsistentRead) and asserts directly instead of retrying.
    const res = await db.send(new QueryCommand({
      TableName: TABLE, IndexName: "byAmount", ConsistentRead: true,
      KeyConditionExpression: "pk = :p AND #a > :a",
      ExpressionAttributeNames: { "#a": "amount" },
      ExpressionAttributeValues: { ":p": { S: "CUST#30" }, ":a": { N: "0" } },
    }));
    expect(res.Count).toBe(1);
  });

  // Unlike an LSI read, a GSI read never supports ConsistentRead: it is always
  // eventually consistent. It lags by a few milliseconds on ExtendDB by default
  // and is allowed to on real DynamoDB. `eventually` is the portable answer.
  it("queries a global secondary index once it has caught up", async () => {
    await db.send(new PutItemCommand({ TableName: TABLE, Item: item(40, "SHIPPED") }));
    const res = await eventually(async () => {
      const r = await db.send(new QueryCommand({
        TableName: TABLE, IndexName: "byStatus",
        KeyConditionExpression: "#s = :s",
        ExpressionAttributeNames: { "#s": "status" },
        ExpressionAttributeValues: { ":s": { S: "SHIPPED" } },
      }));
      if (r.Count === 0) throw new Error("not in index yet");
      return r;
    });
    expect(res.Count).toBe(1);
  });
});

describe("table lifecycle", () => {
  it("enables TTL", async () => {
    const res = await db.send(new UpdateTimeToLiveCommand({
      TableName: TABLE,
      TimeToLiveSpecification: { Enabled: true, AttributeName: "expiresAt" },
    }));
    expect(res.TimeToLiveSpecification?.AttributeName).toBe("expiresAt");
  });
});

describe("signature verification", () => {
  // ExtendDB verifies SigV4; DynamoDB Local accepts any credentials. A suite
  // ported from DynamoDB Local with mismatched dummy keys fails here.
  it("rejects a request signed with the wrong secret", async () => {
    const cfg = inject("ddb");
    const bad = new DynamoDBClient({
      endpoint: cfg.endpoint,
      region: cfg.region,
      credentials: { accessKeyId: cfg.accessKeyId, secretAccessKey: "wrong" },
    });
    await expect(bad.send(new ListTablesCommand({}))).rejects.toMatchObject({ name: "InvalidSignatureException" });
  });
});

describe("test isolation", () => {
  it("gives every call a distinct table name, even for the same suffix", () => {
    expect(tableName("alpha")).not.toBe(tableName("alpha"));
    expect(tableName("alpha")).toMatch(/^extenddb-ci-alpha-/);
  });
});
