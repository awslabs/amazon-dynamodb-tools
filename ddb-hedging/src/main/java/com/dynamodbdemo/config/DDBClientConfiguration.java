package com.dynamodbdemo.config;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DDBClientConfiguration {


    /**
     * Method to overwrite the default SDK client configuration behavior
     *
     * @return ClientConfiguration with custom timeout values and retry method
     */
    @Value("${ddb.client.maxPoolSize}")
    public static int maxPoolSize;

    public static int connectionTimeout = 200; // ms
    public static int clientExecutionTimeout = 200; // ms
    public static int requestTimeout = 200; // ms
    public static int socketTimeout = 100; // 450 ms

    public static int maxErrorRetries = 0;

    public static ClientConfiguration createDynamoDBClientConfiguration() {

        return new ClientConfiguration()
                .withConnectionTimeout(DynamoDBClientParameters.connectionTimeout)
                .withClientExecutionTimeout(DynamoDBClientParameters.clientExecutionTimeout)
                .withRequestTimeout(DynamoDBClientParameters.requestTimeout)
                .withSocketTimeout(DynamoDBClientParameters.socketTimeout)
                .withTcpKeepAlive(true)
                .withMaxConnections(maxPoolSize)
                .withRetryPolicy(PredefinedRetryPolicies
                        .getDynamoDBDefaultRetryPolicyWithCustomMaxRetries(
                                DynamoDBClientParameters.maxErrorRetries));
    }

    @Bean
    public static ClientConfiguration createDynamoDBDefaultClientConfiguration() {

        return new ClientConfiguration();
    }
}
