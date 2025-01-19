package com.dynamodbdemo.model.auth;


import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

@lombok.Data
public class DDBResponse implements DDBMetaDataAccessor {

    private List<Map<String, AttributeValue>> items;

    private long responseLatency;

    private long actualLatency;

    // Initialize to first request.
    private int requestNumber = FIRST_REQUEST;

    private String DDBRequestID;

    private String requestName;

    @Override
    public int getItemCount() {
        if (items == null) {
            return 0;
        } else {
            return items.size();
        }
    }

    @Override
    public long getResponseLatency() {
        return responseLatency;
    }

    @Override
    public int getRequestNumber() {
        return requestNumber;
    }

    @Override
    public String getDDBRequestID() {
        return DDBRequestID;
    }

    @Override
    public String getRequestName() {
        return requestName;
    }

}
