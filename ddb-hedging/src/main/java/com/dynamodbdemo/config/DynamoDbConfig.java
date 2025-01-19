package com.dynamodbdemo.config;

import com.dynamodbdemo.dao.MultiHedgingRequestHandler;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.time.Duration;

@Configuration
public class DynamoDbConfig {

    @Value("${aws.region}")
    private String region;

    @Value("${aws.dynamodb.max-concurrency:100}")
    private int maxConcurrency;

    @Value("${aws.dynamodb.connection-timeout:5}")
    private int connectionTimeoutSeconds;

    @Value("${aws.dynamodb.read-timeout:30}")
    private int readTimeoutSeconds;

    @Value("${aws.dynamodb.write-timeout:30}")
    private int writeTimeoutSeconds;

    @Value("${aws.dynamodb.api-timeout:30}")
    private int apiTimeoutSeconds;

    private DynamoDbAsyncClient dynamoDbAsyncClient;
    private SdkEventLoopGroup eventLoopGroup;

    @Bean
    public SdkEventLoopGroup eventLoopGroup() {
        if (eventLoopGroup == null) {
            eventLoopGroup = SdkEventLoopGroup.builder().build();
        }
        return eventLoopGroup;
    }

    @Bean
    public MultiHedgingRequestHandler dynamoDBRequestHedger() {
        return new MultiHedgingRequestHandler(eventLoopGroup().eventLoopGroup());
    }

    @Bean(name = "DDBASynClient")
    public DynamoDbAsyncClient getDynamoDbAsyncClient() {
        if (dynamoDbAsyncClient == null) {
            software.amazon.awssdk.http.async.SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(maxConcurrency)
                    .connectionTimeout(Duration.ofSeconds(connectionTimeoutSeconds))
                    .readTimeout(Duration.ofSeconds(readTimeoutSeconds))
                    .writeTimeout(Duration.ofSeconds(writeTimeoutSeconds))
                    .eventLoopGroup(eventLoopGroup())
                    .eventLoopGroupBuilder(null) // Disable default event loop group creation
                    .build();

            dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                    .region(Region.of(region))
                    .httpClient(httpClient)
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .apiCallTimeout(Duration.ofSeconds(apiTimeoutSeconds))
                            .build())
                    .build();
        }
        return dynamoDbAsyncClient;
    }

    @Bean
    public DynamoDbEnhancedAsyncClient getDynamoDbEnhancedAsyncClient() {
        return DynamoDbEnhancedAsyncClient.builder()
                .dynamoDbClient(getDynamoDbAsyncClient())
                .build();
    }

    @PreDestroy
    public void cleanUp() {
        if (dynamoDbAsyncClient != null) {
            dynamoDbAsyncClient.close();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.eventLoopGroup().shutdownGracefully()
                    .addListener(future -> {
                        if (future.isSuccess()) {
                            System.out.println("EventLoopGroup shutdown successfully");
                        } else {
                            System.err.println("EventLoopGroup shutdown failed: " + future.cause());
                        }
                    });
        }
    }
}
