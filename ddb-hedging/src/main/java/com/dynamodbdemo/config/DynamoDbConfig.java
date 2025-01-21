package com.dynamodbdemo.config;

import com.dynamodbdemo.dao.CrtHedgingRequestHandler;
import com.dynamodbdemo.dao.NettyHedgingRequestHandler;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(DynamoDbConfig.class);

    @Value("${aws.dynamodb.region}")
    private String region;

    @Value("${aws.dynamodb.connection.timeout-seconds}")
    private int connectionTimeoutSeconds;

    @Value("${aws.dynamodb.api.timeout-seconds}")
    private int apiTimeoutSeconds;

    @Value("${aws.dynamodb.max-concurrency}")
    private int maxConcurrency;

    private DynamoDbAsyncClient dynamoDbAsyncClient;
    private SdkEventLoopGroup eventLoopGroup;

    @Bean
    protected SdkEventLoopGroup createSdkEventLoopGroup() {
        if (this.eventLoopGroup == null) {

            this.eventLoopGroup = SdkEventLoopGroup.builder()
                    .build();
        }
        return this.eventLoopGroup;
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

    @Bean(name = "hedgingRequestHandler")
    @ConditionalOnProperty(name = "aws.dynamodb.use-crt-client", havingValue = "true", matchIfMissing = true)
    public CrtHedgingRequestHandler crtHedgingRequestHandler() {
        logger.info("Initialing CrtHedgingRequestHandler");
        return new CrtHedgingRequestHandler();
    }

    @Bean(name = "hedgingRequestHandler")
    @ConditionalOnProperty(name = "aws.dynamodb.use-crt-client", havingValue = "false")
    public NettyHedgingRequestHandler nettyHedgingRequestHandler(SdkEventLoopGroup eventLoopGroup) {
        logger.info("Initialing NettyHedgingRequestHandler");
        return new NettyHedgingRequestHandler(eventLoopGroup.eventLoopGroup());
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
