import { randomUUID } from "node:crypto";
import { inject } from "vitest";
import {
  DynamoDBClient, CreateTableCommand, waitUntilTableExists,
} from "@aws-sdk/client-dynamodb";

/** A DynamoDB client pointed at whichever server the harness provided. */
export function client() {
  const cfg = inject("ddb");
  return new DynamoDBClient({
    endpoint: cfg.endpoint,
    region: cfg.region,
    credentials: {
      accessKeyId: cfg.accessKeyId,
      secretAccessKey: cfg.secretAccessKey,
    },
  });
}

/**
 * A table name unique to this test file and run, so parallel shards sharing one
 * server never collide. The random component covers same-millisecond calls and
 * parallel CI containers that share pids. The same habit works against real DynamoDB.
 */
export function tableName(suffix) {
  return `extenddb-ci-${suffix}-${process.pid}-${Date.now()}-${randomUUID().slice(0, 8)}`;
}

/**
 * Create the suite's table and wait until it is usable.
 *
 * ExtendDB returns CREATING from CreateTable, as the real service does, so a
 * write issued immediately fails with ResourceNotFoundException. The SDK's own
 * waiter is the portable fix: this same code is correct against DynamoDB Local
 * and against real DynamoDB.
 */
export async function createTable(db, name) {
  await db.send(new CreateTableCommand({
    TableName: name,
    BillingMode: "PAY_PER_REQUEST",
    AttributeDefinitions: [
      { AttributeName: "pk", AttributeType: "S" },
      { AttributeName: "sk", AttributeType: "S" },
      { AttributeName: "status", AttributeType: "S" },
      { AttributeName: "createdAt", AttributeType: "S" },
      { AttributeName: "amount", AttributeType: "N" },
    ],
    KeySchema: [
      { AttributeName: "pk", KeyType: "HASH" },
      { AttributeName: "sk", KeyType: "RANGE" },
    ],
    GlobalSecondaryIndexes: [{
      IndexName: "byStatus",
      KeySchema: [
        { AttributeName: "status", KeyType: "HASH" },
        { AttributeName: "createdAt", KeyType: "RANGE" },
      ],
      Projection: { ProjectionType: "ALL" },
    }],
    LocalSecondaryIndexes: [{
      IndexName: "byAmount",
      KeySchema: [
        { AttributeName: "pk", KeyType: "HASH" },
        { AttributeName: "amount", KeyType: "RANGE" },
      ],
      Projection: { ProjectionType: "ALL" },
    }],
  }));
  // The DynamoDB waiter spec's default floor is 20 s between polls, which would
  // dominate a local test run. These values keep the wait proportional to a
  // local server, where a table leaves CREATING in about a second.
  await waitUntilTableExists({ client: db, maxWaitTime: 30, minDelay: 1, maxDelay: 2 }, { TableName: name });
}

/**
 * Retry an assertion until it stops throwing.
 *
 * Global secondary index reads can lag their writes: on ExtendDB by its default
 * propagation delay, and on real DynamoDB they are allowed to. Polling is the portable way to
 * assert on an index without a sleep that is either flaky or slow.
 */
export async function eventually(fn, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
  }
  throw lastError;
}
