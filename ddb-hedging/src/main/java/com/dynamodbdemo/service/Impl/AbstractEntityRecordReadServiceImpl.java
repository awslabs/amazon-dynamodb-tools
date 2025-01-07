package com.dynamodbdemo.service.Impl;

import com.dynamodbdemo.model.auth.DDBMetaDataAccessor;
import com.dynamodbdemo.model.auth.RequestDTO;
import com.dynamodbdemo.service.BL.EntityRecordReadServiceBL;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class AbstractEntityRecordReadServiceImpl implements EntityRecordReadServiceBL {
    Logger logger = Logger.getLogger(AbstractEntityRecordReadServiceImpl.class.getName());

    @Value("${ddb.hedging.request.delay}")
    protected int delayInMillis;

    @Override
    public RequestDTO transactRecords(RequestDTO requestDTO) throws Exception {

        logger.log(Level.FINE, "transactRecords - Start " + Thread.currentThread());

        long startTime = System.currentTimeMillis();

        String recordId = requestDTO.getRecordId();

        String entityNumber = requestDTO.getEntityNumber();


        List<DDBMetaDataAccessor> metaDataAccessorCCAuthResponse = getEntityRecords(recordId, entityNumber, delayInMillis);

        AtomicInteger totalItems = new AtomicInteger();

        metaDataAccessorCCAuthResponse.forEach(dataAccessor -> {
            totalItems.set(totalItems.get() + dataAccessor.getItemCount());
        });

        requestDTO.setItemCount(totalItems.get());

        long endTime = System.currentTimeMillis();

        PrintLog(endTime - startTime, requestDTO, metaDataAccessorCCAuthResponse);

        logger.log(Level.FINE, "transactRecords - End");

        return requestDTO;
    }


    void PrintLog(Long totalTime, RequestDTO requestDTO, List<DDBMetaDataAccessor> metaDataAccesors) {

        StringBuilder LogMessage = new StringBuilder();

        metaDataAccesors.forEach(dataAccessor -> LogMessage.append(dataAccessor.getRequestNumber()).append(":").append(dataAccessor.getResponseLatency()).append(":").append(dataAccessor.getDDBRequestID()).append(":").append(requestDTO.getEntityNumber()).append("-").append(requestDTO.getRecordId()).append(":"));

        LogMessage.append(totalTime);

        logger.log(Level.INFO, LogMessage.toString());

    }


    public abstract List<DDBMetaDataAccessor> getEntityRecords(String recordId, String entityNumber, int delayInMillis) throws ExecutionException, InterruptedException;
}
