package com.dynamodbdemo.config;

import io.netty.channel.EventLoopGroup;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.time.Duration;

@Configuration
public class DynamoDbConfig {

    @Value("${aws.dynamodb.region}")
    private String region;

    @Value("${aws.dynamodb.connection.timeout-seconds}")
    private int connectionTimeoutSeconds;

    @Value("${aws.dynamodb.api.timeout-seconds}")
    private int apiTimeoutSeconds;

    @Value("${aws.dynamodb.max-concurrency}")
    private int maxConcurrency;

    private DynamoDbAsyncClient dynamoDbAsyncClient;
    private EventLoopGroup eventLoopGroup;

    private SdkEventLoopGroup sdkEventLoopGroup;

    @Bean
    protected EventLoopGroup createEventLoopGroup() {
        if (this.eventLoopGroup == null) {

            if (this.sdkEventLoopGroup == null) {

                this.sdkEventLoopGroup = SdkEventLoopGroup.builder()
                        .build();
            }
            this.eventLoopGroup = this.sdkEventLoopGroup.eventLoopGroup();
        }
        return this.eventLoopGroup;
    }

    @Bean
    protected SdkEventLoopGroup createSdkEventLoopGroup() {
        if (this.sdkEventLoopGroup == null) {

            this.sdkEventLoopGroup = SdkEventLoopGroup.builder()
                    .build();
        }
        return this.sdkEventLoopGroup;
    }

    @Bean(name = "DDBAsyncClient")
    @ConditionalOnProperty(name = "aws.dynamodb.use-crt-client", havingValue = "true", matchIfMissing = true)
    public DynamoDbAsyncClient getCrtDynamoDbAsyncClient() {
        if (dynamoDbAsyncClient == null) {
            dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                    .region(Region.of(region))
                    .httpClient(AwsCrtAsyncHttpClient.builder()
                            .maxConcurrency(maxConcurrency)
                            .connectionTimeout(Duration.ofSeconds(connectionTimeoutSeconds))
                            .build())
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .apiCallTimeout(Duration.ofSeconds(apiTimeoutSeconds))
                            .build())
                    .build();
        }
        return dynamoDbAsyncClient;
    }

    @Bean(name = "DDBAsyncClient")
    @ConditionalOnProperty(name = "aws.dynamodb.use-crt-client", havingValue = "false")
    public DynamoDbAsyncClient getNettyDynamoDbAsyncClient(SdkEventLoopGroup eventLoopGroup) {
        if (dynamoDbAsyncClient == null) {

            dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                    .region(Region.of(region))
                    .httpClient(NettyNioAsyncHttpClient.builder()
                            .maxConcurrency(maxConcurrency)
                            .connectionTimeout(Duration.ofSeconds(connectionTimeoutSeconds))
                            .eventLoopGroup(eventLoopGroup)
                            .build())
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .apiCallTimeout(Duration.ofSeconds(apiTimeoutSeconds))
                            .build())
                    .build();
        }
        return dynamoDbAsyncClient;
    }

    @PreDestroy
    public void cleanUp() {
        if (dynamoDbAsyncClient != null) {
            dynamoDbAsyncClient.close();
        }
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully()
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
