package com.dynamodbdemo.model;

import lombok.Data;

@Data
public class RequestDTO {

    private String transId;
    private String recordId;
    private int itemCount;
    private String entityNumber;

    public void setError(String message) {
    }
}
