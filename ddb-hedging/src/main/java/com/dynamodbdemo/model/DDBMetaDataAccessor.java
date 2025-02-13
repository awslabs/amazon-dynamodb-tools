package com.dynamodbdemo.model;

public interface DDBMetaDataAccessor {

    int FIRST_REQUEST = 1;

    int getItemCount();

    long getResponseLatency();

    int getRequestNumber();

    long getActualLatency();

    String getDDBRequestID();

}
