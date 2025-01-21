package com.dynamodbdemo.dao;

import com.dynamodbdemo.model.auth.DDBResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.logging.Level;
import java.util.logging.Logger;

@Service
public class EntityRecordDDbNativeDAO {
    private static final Logger logger = Logger.getLogger(EntityRecordDDbNativeDAO.class.getName());

    private final DynamoDbAsyncClient asyncClient;

    @Value("${aws.dynamodb.table-name}")
    private String tableName;

    public EntityRecordDDbNativeDAO(@Qualifier("DDBAsyncClient") DynamoDbAsyncClient ddbClient) {
        this.asyncClient = ddbClient;
    }

    public CompletableFuture<DDBResponse> fetchByRecordIDAndEntityNumberAsync(String recordID, String entityNumber) {
        logger.log(Level.FINE, "fetchByRecordIDAndEntityNumberAsync - Start");

        String pk_EntityNumRecordID = entityNumber + "-" + recordID;

        // Build query request
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("PK = :pk_EntityNumRecordID")
                .expressionAttributeValues(Map.of(
                        ":pk_EntityNumRecordID", AttributeValue.builder().s(pk_EntityNumRecordID).build()
                ))
                .build();

        long startTime = System.currentTimeMillis();

        // Execute async query and transform the response
        return asyncClient.query(queryRequest)
                .thenApply(queryResponse -> {
                    long endTime = System.currentTimeMillis();

                    DDBResponse ddbResponse = new DDBResponse();
                    ddbResponse.setItems(queryResponse.items());
                    ddbResponse.setRequestName("fetchByRecordIDAndEntityNumberAsync");
                    ddbResponse.setResponseLatency(endTime - startTime);
                    ddbResponse.setDDBRequestID(queryResponse.responseMetadata().requestId());

                    logger.log(Level.FINE, "fetchByRecordIDAndEntityNumberAsync - End");
                    return ddbResponse;
                })
                .exceptionally(throwable -> {
                    logger.log(Level.SEVERE, "fetchByRecordIDAndEntityNumberAsync - Error ", throwable);
                    throw new CompletionException(throwable);
                });
    }
}
