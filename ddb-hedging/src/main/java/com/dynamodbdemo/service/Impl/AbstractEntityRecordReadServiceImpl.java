package com.dynamodbdemo.service.Impl;

import com.dynamodbdemo.model.DDBMetaDataAccessor;
import com.dynamodbdemo.model.RequestDTO;
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
    protected float delayInMillis;

    @Value("${ddb.hedging.number}")
    protected int numberOfHedgers;

    @Override
    public RequestDTO transactRecords(RequestDTO requestDTO) throws Exception {

        logger.log(Level.FINE, "transactRecords - Start " + Thread.currentThread());

        long startTime = System.nanoTime();

        String recordId = requestDTO.getRecordId();

        String entityNumber = requestDTO.getEntityNumber();


        List<DDBMetaDataAccessor> metaDataAccessorCCAuthResponse = getEntityRecords(recordId, entityNumber, delayInMillis, numberOfHedgers);

        AtomicInteger totalItems = new AtomicInteger();

        metaDataAccessorCCAuthResponse.forEach(dataAccessor -> totalItems.set(totalItems.get() + dataAccessor.getItemCount()));

        requestDTO.setItemCount(totalItems.get());

        long endTime = System.nanoTime();

        PrintLog(endTime - startTime, requestDTO, metaDataAccessorCCAuthResponse);

        logger.log(Level.FINE, "transactRecords - End");

        return requestDTO;
    }


    void PrintLog(Long totalTime, RequestDTO requestDTO, List<DDBMetaDataAccessor> metaDataAccesors) {

        StringBuilder LogMessage = new StringBuilder();

        metaDataAccesors.forEach(dataAccessor -> LogMessage.append(dataAccessor.getRequestNumber()).append(":").append(String.format("%.2f", (float) dataAccessor.getResponseLatency() /1000000)).append(":").append(dataAccessor.getDDBRequestID()).append(":").append(requestDTO.getEntityNumber()).append("-").append(requestDTO.getRecordId()).append(":"));

        float result = (float) totalTime / 1000000;
        LogMessage.append(String.format("%.2f", result));


        logger.log(Level.INFO, LogMessage.toString());

    }


    public abstract List<DDBMetaDataAccessor> getEntityRecords(String recordId, String entityNumber, float delayInMillis, int numberOfHedgers) throws ExecutionException, InterruptedException;
}
