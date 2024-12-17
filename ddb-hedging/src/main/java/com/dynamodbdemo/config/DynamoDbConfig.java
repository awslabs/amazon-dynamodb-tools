package com.dynamodbdemo.config;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DynamoDbConfig {

    @Value("${aws.ddb.tablename}")
    public String ddbTableName;

    @Value("${aws.metrics.publish.enabled}")
    public boolean metricsPublishEnabled = false;

    @Value("${aws.region}")
    public String region;


    final ClientConfiguration clientConfiguration;

    public DynamoDbConfig(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
    }


    @Bean(name = "DDBSynClient")
    public AmazonDynamoDB getWebClient() {

        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(region)
                .withClientConfiguration(clientConfiguration)
                .build();

    }


}
