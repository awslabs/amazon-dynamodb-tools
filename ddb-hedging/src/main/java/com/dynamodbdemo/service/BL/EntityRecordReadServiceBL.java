package com.dynamodbdemo.service.BL;

import com.dynamodbdemo.model.RequestDTO;

import java.util.concurrent.CompletableFuture;

public interface EntityRecordReadServiceBL {

    RequestDTO transactRecords(RequestDTO requestDTO) throws Exception;

    CompletableFuture<RequestDTO> transactRecordsAsync(RequestDTO requestDTO);

}
