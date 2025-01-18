package com.dynamodbdemo.service.Impl;

import com.dynamodbdemo.dao.EntityRecordDDbNativeDAO;
import com.dynamodbdemo.dao.SimpleHedgingRequestHandler;
import com.dynamodbdemo.dao.MultiHedgingRequestHandler;
import com.dynamodbdemo.model.auth.DDBMetaDataAccessor;
import com.dynamodbdemo.model.auth.DDBResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service("EntityRecordReadServiceDDbNativeHedgingImpl")
@Configuration
public class EntityRecordReadServiceDDbNativeHedgingImpl extends AbstractEntityRecordReadServiceImpl {


    @Value("${ddb.hedging.simple}")
    public boolean useSimpleHedging;
    private final EntityRecordDDbNativeDAO entityRecordDDbNativeDAO;

    public EntityRecordReadServiceDDbNativeHedgingImpl(EntityRecordDDbNativeDAO entityRecordDDbNativeDAO) {
        this.entityRecordDDbNativeDAO = entityRecordDDbNativeDAO;
    }

    @Override
    public List<DDBMetaDataAccessor> getEntityRecords(String ccNum, String clientId, int delayInMillis, int numberOfHedgers) throws ExecutionException, InterruptedException {

        long startTime = System.currentTimeMillis();



        DDBResponse clientIDAndAppNumResponseItems = null;

        if(useSimpleHedging) {
            clientIDAndAppNumResponseItems = getDdbResponse(ccNum, clientId, delayInMillis);
        } else {
            clientIDAndAppNumResponseItems = getDdbResponse(ccNum, clientId, delayInMillis, numberOfHedgers);
        }

        long endTime = System.currentTimeMillis();
        clientIDAndAppNumResponseItems.setActualLatency(endTime - startTime);

        List<DDBMetaDataAccessor> metaDataAccessor = new ArrayList<>();
        metaDataAccessor.add(clientIDAndAppNumResponseItems);

        return metaDataAccessor;
    }

    private DDBResponse getDdbResponse(String ccNum, String clientId, int delayInMillis) throws InterruptedException, ExecutionException {
        SimpleHedgingRequestHandler simpleHedgingRequestHandler = new SimpleHedgingRequestHandler();

        CompletableFuture<DDBResponse> fetchByClientIDAndAppNumResponseFuture = simpleHedgingRequestHandler.hedgeRequest(() -> {
            DDBResponse ddbResponse = entityRecordDDbNativeDAO.fetchByRecordIDAndEntityNumber(ccNum, clientId);
            ddbResponse.setRequestNumber(DDBMetaDataAccessor.FIRST_REQUEST);
            return ddbResponse;

        }, delayInMillis);


        return fetchByClientIDAndAppNumResponseFuture.get();
    }


    private DDBResponse getDdbResponse(String ccNum, String clientId, int delayInMillis, int numberOfHedgers) throws InterruptedException, ExecutionException {
        MultiHedgingRequestHandler hedgingRequestHandler = new MultiHedgingRequestHandler();

        CompletableFuture<DDBResponse> fetchByClientIDAndAppNumResponseFuture = hedgingRequestHandler.hedgeRequest(() -> {
            return entityRecordDDbNativeDAO.fetchByRecordIDAndEntityNumber(ccNum, clientId);

        }, delayInMillis, numberOfHedgers);


        return fetchByClientIDAndAppNumResponseFuture.get();
    }
}
