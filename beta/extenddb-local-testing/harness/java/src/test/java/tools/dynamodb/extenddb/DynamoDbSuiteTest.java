package tools.dynamodb.extenddb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/** The shared suite, in Java. Mirrors the TypeScript and Python suites test for test. */
class DynamoDbSuiteTest {

  private static DynamoDbClient db;
  private static String table;

  @BeforeAll
  static void setUp() {
    db = ExtendDbSupport.client();
    table = ExtendDbSupport.tableName("java");
    ExtendDbSupport.createTable(db, table);
  }

  @AfterAll
  static void tearDown() {
    if (db == null) {
      return; // setUp failed; let its own error surface
    }
    try {
      db.deleteTable(DeleteTableRequest.builder().tableName(table).build());
    } finally {
      db.close();
    }
  }

  private static Map<String, AttributeValue> item(int n, String status) {
    return Map.of(
        "pk", AttributeValue.fromS("CUST#" + n),
        "sk", AttributeValue.fromS("ORDER#" + n),
        "status", AttributeValue.fromS(status),
        "createdAt", AttributeValue.fromS(String.format("2026-01-01T00:00:%02dZ", n)),
        "amount", AttributeValue.fromN(String.valueOf(n * 10)));
  }

  private static Map<String, AttributeValue> key(int n) {
    return Map.of(
        "pk", AttributeValue.fromS("CUST#" + n),
        "sk", AttributeValue.fromS("ORDER#" + n));
  }

  @Test
  void writesAndReadsAnItem() {
    db.putItem(r -> r.tableName(table).item(item(1, "NEW")));
    var res = db.getItem(r -> r.tableName(table).key(key(1)).consistentRead(true));
    assertEquals("NEW", res.item().get("status").s());
  }

  @Test
  void rejectsAWriteWhoseConditionFails() {
    db.putItem(r -> r.tableName(table).item(item(2, "NEW")));
    assertThrows(
        ConditionalCheckFailedException.class,
        () -> db.putItem(r -> r.tableName(table)
            .item(item(2, "NEW"))
            .conditionExpression("attribute_not_exists(pk)")));
  }

  @Test
  void writesABatch() {
    List<WriteRequest> writes = List.of(3, 4, 5).stream()
        .map(n -> WriteRequest.builder()
            .putRequest(PutRequest.builder().item(item(n, "NEW")).build())
            .build())
        .toList();
    db.batchWriteItem(r -> r.requestItems(Map.of(table, writes)));
    for (int n : List.of(3, 4, 5)) {
      var res = db.getItem(r -> r.tableName(table).key(key(n)).consistentRead(true));
      assertTrue(res.hasItem(), "item " + n + " should exist");
    }
  }

  @Test
  void transactionCommitsBothMembers() {
    db.transactWriteItems(TransactWriteItemsRequest.builder()
        .transactItems(
            TransactWriteItem.builder()
                .put(Put.builder().tableName(table).item(item(10, "PAID")).build())
                .build(),
            TransactWriteItem.builder()
                .put(Put.builder().tableName(table).item(item(11, "PAID")).build())
                .build())
        .build());
    var res = db.getItem(r -> r.tableName(table).key(key(10)).consistentRead(true));
    assertEquals("PAID", res.item().get("status").s());
  }

  @Test
  void transactionCancelsWhenOneConditionFails() {
    assertThrows(
        TransactionCanceledException.class,
        () -> db.transactWriteItems(TransactWriteItemsRequest.builder()
            .transactItems(
                TransactWriteItem.builder()
                    .put(Put.builder().tableName(table).item(item(12, "NEW")).build())
                    .build(),
                TransactWriteItem.builder()
                    .put(Put.builder()
                        .tableName(table)
                        .item(item(13, "NEW"))
                        .conditionExpression("attribute_exists(pk)")
                        .build())
                    .build())
            .build()));
    var res = db.getItem(r -> r.tableName(table).key(key(12)).consistentRead(true));
    assertFalse(res.hasItem());
  }

  @Test
  void queriesTheBaseTable() {
    db.putItem(r -> r.tableName(table).item(item(20, "NEW")));
    var res = db.query(QueryRequest.builder()
        .tableName(table)
        .keyConditionExpression("pk = :p AND begins_with(sk, :s)")
        .expressionAttributeValues(Map.of(
            ":p", AttributeValue.fromS("CUST#20"),
            ":s", AttributeValue.fromS("ORDER#")))
        .build());
    assertEquals(1, res.count());
  }

  @Test
  void queriesALocalSecondaryIndex() {
    db.putItem(r -> r.tableName(table).item(item(30, "NEW")));
    // An LSI read can be strongly consistent when asked for, so this test asks
    // (consistentRead) and asserts directly instead of retrying.
    var res = db.query(QueryRequest.builder()
        .tableName(table)
        .indexName("byAmount")
        .consistentRead(true)
        .keyConditionExpression("pk = :p AND #a > :a")
        .expressionAttributeNames(Map.of("#a", "amount"))
        .expressionAttributeValues(Map.of(
            ":p", AttributeValue.fromS("CUST#30"),
            ":a", AttributeValue.fromN("0")))
        .build());
    assertEquals(1, res.count());
  }

  // Unlike an LSI read, a GSI read never supports ConsistentRead: it is always
  // eventually consistent. It lags by about 10 ms on ExtendDB by default
  // and is allowed to on real DynamoDB. `eventually` is the portable answer.
  @Test
  void queriesAGlobalSecondaryIndexOnceCaughtUp() {
    db.putItem(r -> r.tableName(table).item(item(40, "SHIPPED")));
    ExtendDbSupport.eventually(() -> {
      QueryResponse r = db.query(QueryRequest.builder()
          .tableName(table)
          .indexName("byStatus")
          .keyConditionExpression("#s = :s")
          .expressionAttributeNames(Map.of("#s", "status"))
          .expressionAttributeValues(Map.of(":s", AttributeValue.fromS("SHIPPED")))
          .build());
      assertEquals(1, r.count());
      return r;
    });
  }

  @Test
  void enablesTtl() {
    UpdateTimeToLiveResponse res = db.updateTimeToLive(UpdateTimeToLiveRequest.builder()
        .tableName(table)
        .timeToLiveSpecification(TimeToLiveSpecification.builder()
            .enabled(true)
            .attributeName("expiresAt")
            .build())
        .build());
    assertEquals("expiresAt", res.timeToLiveSpecification().attributeName());
  }

  @Test
  void rejectsARequestSignedWithTheWrongSecret() {
    // ExtendDB verifies SigV4; DynamoDB Local accepts any credentials. A suite
    // ported from DynamoDB Local with mismatched dummy keys fails here. The key
    // id is a known one, so the server reaches signature checking and reports
    // InvalidSignatureException (an unknown key id would give
    // UnrecognizedClientException instead).
    try (DynamoDbClient bad = ExtendDbSupport.clientWithSecret("wrong")) {
      AwsServiceException thrown = assertThrows(AwsServiceException.class, bad::listTables);
      assertEquals("InvalidSignatureException", thrown.awsErrorDetails().errorCode());
    }
  }

  @Test
  void tableNamesAreDistinctPerCallEvenForTheSameSuffix() {
    assertNotEquals(ExtendDbSupport.tableName("alpha"), ExtendDbSupport.tableName("alpha"));
    assertTrue(ExtendDbSupport.tableName("alpha").startsWith("extenddb-ci-alpha-"));
  }
}
