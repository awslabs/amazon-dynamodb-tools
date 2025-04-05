package com.dynamodbdemo.config;

import io.netty.channel.EventLoopGroup;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;

import software.amazon.awssdk.core.exception.SdkClientException;
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
        @Profile("!local") // Use for non-local environments (EC2)
        public AwsCredentialsProvider ec2CredentialsProvider() {
            return DefaultCredentialsProvider.create();
        }

        @Bean
        @Profile("local") // Use for local development
        public AwsCredentialsProvider localCredentialsProvider(
                @Value("${aws.profile-name:PowerUserAccess-278322016574}") String profileName) {
            return ProfileCredentialsProvider.builder()
                    .profileName(profileName)
                    .build();
        }



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
    public DynamoDbAsyncClient getCrtDynamoDbAsyncClient(AwsCredentialsProvider credentialsProvider) {
        try {
            if (dynamoDbAsyncClient == null) {
                dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(credentialsProvider)
                        .httpClient(AwsCrtAsyncHttpClient.builder()
                                .maxConcurrency(maxConcurrency)
                                .connectionTimeout(Duration.ofSeconds(connectionTimeoutSeconds))
                                .build())
                        .overrideConfiguration(ClientOverrideConfiguration.builder()
                                .apiCallTimeout(Duration.ofSeconds(apiTimeoutSeconds))
                                .build())
                        .build();
            }
        } catch (SdkClientException e) {
            throw new IllegalStateException("Failed to initialize DynamoDB client due to credential issues", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize DynamoDB client", e);
        }
        return dynamoDbAsyncClient;
    }

    @Bean(name = "DDBAsyncClient")
    @ConditionalOnProperty(name = "aws.dynamodb.use-crt-client", havingValue = "false")
    public DynamoDbAsyncClient getNettyDynamoDbAsyncClient(SdkEventLoopGroup eventLoopGroup, AwsCredentialsProvider credentialsProvider) {
        try {
            if (dynamoDbAsyncClient == null) {

                dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(credentialsProvider)
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
        } catch (SdkClientException e) {
            throw new IllegalStateException("Failed to initialize DynamoDB client due to credential issues", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize DynamoDB client", e);
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
