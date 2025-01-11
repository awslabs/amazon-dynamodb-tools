package com.dynamodbdemo.dao;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.dynamodbdemo.config.DynamoDbConfig;
import com.dynamodbdemo.model.auth.DDBResponse;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@Service
public class EntityRecordDDbNativeDAO {

    Logger logger = Logger.getLogger(EntityRecordDDbNativeDAO.class.getName());

    final
    DynamoDbConfig config;

    final
    AmazonDynamoDB ddb;

    public EntityRecordDDbNativeDAO(DynamoDbConfig config, AmazonDynamoDB ddb) {
        this.config = config;
        this.ddb = ddb;
    }


    public DDBResponse fetchByRecordIDAndEntityNumber(String recordID, String entityNumber) {

        logger.log(Level.FINE, "fetchByRecordIDAndEntityNumber - Start");

        DDBResponse ddbResponse = new DDBResponse();

        List<Map<String, AttributeValue>> fetchedItems;

        try {
            String pk_EntityNumRecordID = entityNumber + "-" + recordID;

            HashMap<String, AttributeValue> fetchByRecordIDAndEntityNumberValues = new HashMap<>();
            fetchByRecordIDAndEntityNumberValues.put(":pk_EntityNumRecordID", new AttributeValue().withS(pk_EntityNumRecordID));

            // get all other data
            QueryRequest fetchByRecordIDAndEntityNumberReq = new QueryRequest()
                    .withTableName(config.ddbTableName)
                    .withKeyConditionExpression("PK = :pk_EntityNumRecordID")
                    .withExpressionAttributeValues(fetchByRecordIDAndEntityNumberValues);

            long startTime = System.currentTimeMillis();

            QueryResult fetchByRecordIDAndEntityNumberResponse = ddb.query(fetchByRecordIDAndEntityNumberReq);
            fetchedItems = fetchByRecordIDAndEntityNumberResponse.getItems();

            long endTime = System.currentTimeMillis();

            ddbResponse.setItems(fetchedItems);
            ddbResponse.setRequestName("fetchByRecordIDAndEntityNumber");
            ddbResponse.setResponseLatency(endTime - startTime);
            ddbResponse.setDDBRequestID(fetchByRecordIDAndEntityNumberResponse.getSdkResponseMetadata().getRequestId());
            logger.log(Level.FINE, "fetchByRecordIDAndEntityNumber - End ");

        } catch (Exception e) {
            logger.log(Level.SEVERE, "fetchByRecordIDAndEntityNumber - Error ", e);
            throw new RuntimeException(e);
        }
        return ddbResponse;
    }

}
