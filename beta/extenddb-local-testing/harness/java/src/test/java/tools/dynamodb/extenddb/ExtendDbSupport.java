package tools.dynamodb.extenddb;

import java.net.URI;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Gives the suite a DynamoDB endpoint, however the environment supplies one.
 *
 * <p>In CI with a service container, {@code DDB_ENDPOINT} is already set and this
 * class uses it. On a laptop with nothing running, it starts the ExtendDB dev
 * container through Testcontainers. There is no Maven artifact that embeds the
 * server, so the JVM uses the container.
 */
public final class ExtendDbSupport {

  private static final String IMAGE = "extenddb/extenddb-dev:0.1.14";
  private static final String EXAMPLE_KEY_ID = "AKIAIOSFODNN7EXAMPLE";
  private static final String EXAMPLE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
  private static final int CONTAINER_PORT = 18080;

  private static GenericContainer<?> container;
  private static String endpoint;

  private ExtendDbSupport() {}

  /** The endpoint the suite talks to; starts a container only if needed. */
  public static synchronized String endpoint() {
    if (endpoint != null) {
      return endpoint;
    }
    String supplied = System.getenv("DDB_ENDPOINT");
    if (supplied != null && !supplied.isBlank()) {
      endpoint = supplied;
      return endpoint;
    }
    // Wait on the image's own HEALTHCHECK. The image is distroless and has no
    // shell, so a custom health command could not run.
    container = new GenericContainer<>(IMAGE)
        .withExposedPorts(CONTAINER_PORT)
        .withEnv("EXTENDDB__STORAGE__SQLITE__PATH", ":memory:")
        .waitingFor(Wait.forHealthcheck().withStartupTimeout(Duration.ofSeconds(60)));
    // Register the stop hook before start() so a failed start still cleans up.
    // Testcontainers' Ryuk also cleans up, unless TESTCONTAINERS_RYUK_DISABLED=true.
    GenericContainer<?> started = container;
    Runtime.getRuntime().addShutdownHook(new Thread(started::stop));
    started.start();
    endpoint = "http://" + container.getHost() + ":" + container.getMappedPort(CONTAINER_PORT);
    return endpoint;
  }

  public static DynamoDbClient client() {
    String[] pair = credentials();
    return build(pair[0], pair[1]);
  }

  /** The (key id, secret) pair the suite signs with. */
  static String[] credentials() {
    String keyId = System.getenv("AWS_ACCESS_KEY_ID");
    String secret = System.getenv("AWS_SECRET_ACCESS_KEY");
    if (keyId != null && !keyId.isBlank() && secret != null && !secret.isBlank()) {
      return new String[] {keyId, secret};
    }
    // Only use the environment pair if both halves are set.
    return new String[] {EXAMPLE_KEY_ID, EXAMPLE_SECRET};
  }

  /**
   * A client with a deliberately wrong secret, for the signature test. It keeps
   * the suite's key id, which is a known one, so the server reaches signature checking.
   */
  public static DynamoDbClient clientWithSecret(String secret) {
    return build(credentials()[0], secret);
  }

  private static DynamoDbClient build(String keyId, String secret) {
    return DynamoDbClient.builder()
        .endpointOverride(URI.create(endpoint()))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(keyId, secret)))
        .build();
  }

  /**
   * A table name unique to this call, so parallel shards sharing one server get
   * separate tables.
   */
  public static String tableName(String suffix) {
    return "extenddb-ci-" + suffix + "-" + UUID.randomUUID().toString().substring(0, 12);
  }

  /**
   * Create the suite's table and wait until it is usable.
   *
   * <p>ExtendDB returns CREATING from CreateTable, as the real service does, so a
   * write issued immediately fails with ResourceNotFoundException. The SDK waiter
   * handles that, against DynamoDB Local and real DynamoDB too.
   */
  public static void createTable(DynamoDbClient db, String name) {
    db.createTable(CreateTableRequest.builder()
        .tableName(name)
        .billingMode(BillingMode.PAY_PER_REQUEST)
        .attributeDefinitions(
            AttributeDefinition.builder()
                .attributeName("pk").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder()
                .attributeName("sk").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder()
                .attributeName("status").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder()
                .attributeName("createdAt").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder()
                .attributeName("amount").attributeType(ScalarAttributeType.N).build())
        .keySchema(
            KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
        .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
            .indexName("byStatus")
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName("status").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder()
                    .attributeName("createdAt").keyType(KeyType.RANGE).build())
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .build())
        .localSecondaryIndexes(LocalSecondaryIndex.builder()
            .indexName("byAmount")
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName("pk").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder()
                    .attributeName("amount").keyType(KeyType.RANGE).build())
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .build())
        .build());
    // The SDK inherits the DynamoDB waiter spec's default of 20 s between polls,
    // which would dominate a local run. A local table leaves CREATING in well
    // under a second, so poll every second.
    db.waiter().waitUntilTableExists(
        r -> r.tableName(name),
        o -> o.maxAttempts(30).backoffStrategy(FixedDelayBackoffStrategy.create(Duration.ofSeconds(1))));
  }

  /**
   * Retry an assertion until it stops throwing.
   *
   * <p>GSI reads can lag writes, on ExtendDB and on real DynamoDB. Poll instead
   * of sleeping.
   */
  public static <T> T eventually(Supplier<T> fn) {
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    AssertionError last = null;
    while (System.nanoTime() < deadline) {
      try {
        return fn.get();
      } catch (AssertionError error) {
        last = error;
        try {
          Thread.sleep(25);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(interrupted);
        }
      }
    }
    throw last;
  }
}
