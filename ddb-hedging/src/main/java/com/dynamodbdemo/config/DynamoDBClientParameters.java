package com.dynamodbdemo.config;

public interface DynamoDBClientParameters {
    /**
     * Settings for timeouts
     * <p>
     * Ref:
     * https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/ClientConfiguration.java
     * <p>
     * DEFAULT_CONNECTION_TIMEOUT - 10 s
     * DEFAULT_CLIENT_EXECUTION_TIMEOUT - 0 i.e., disabled
     * DEFAULT_REQUEST_TIMEOUT - 0 i.e., disabled
     * DEFAULT_SOCKET_TIMEOUT - 50 s
     */
    int connectionTimeout = 200; // ms
    int clientExecutionTimeout = 200; // ms
    int requestTimeout = 200; // ms
    int socketTimeout = 100; // 450 ms

    int maxErrorRetries = 0;

}
