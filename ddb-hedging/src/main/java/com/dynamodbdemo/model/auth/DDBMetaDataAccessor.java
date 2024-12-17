package com.dynamodbdemo.model.auth;

public interface DDBMetaDataAccessor {

    int FIRST_REQUEST = 1;
    int SECOND_REQUEST = 2;

    int getItemCount();

    long getResponseLatency();

    int getRequestNumber();

    long getActualLatency();

    String getDDBRequestID();

    String getRequestName();

}
