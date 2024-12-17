package com.dynamodbdemo.model.auth;

import lombok.Data;

@Data
public class RequestDTO {

    private String transId;
    private String recordId;
    private int itemCount;
    private String entityNumber;

}
