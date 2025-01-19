package com.dynamodbdemo.service.Impl;

import com.dynamodbdemo.dao.EntityRecordDDbNativeDAO;
import com.dynamodbdemo.dao.MultiHedgingRequestHandler;
import com.dynamodbdemo.dao.SimpleHedgingRequestHandler;
import com.dynamodbdemo.model.auth.DDBMetaDataAccessor;
import com.dynamodbdemo.model.auth.DDBResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service("EntityRecordReadServiceDDbNativeHedgingImpl")
@Configuration
public class EntityRecordReadServiceDDbNativeHedgingImpl extends AbstractEntityRecordReadServiceImpl {

    private static final Logger logger = LoggerFactory.getLogger(EntityRecordReadServiceDDbNativeHedgingImpl.class);

    @Value("${ddb.hedging.simple:true}")
    private boolean useSimpleHedging;

    private final EntityRecordDDbNativeDAO entityRecordDDbNativeDAO;

    private final MultiHedgingRequestHandler multiHedgingRequestHandler;

    public EntityRecordReadServiceDDbNativeHedgingImpl(
            EntityRecordDDbNativeDAO entityRecordDDbNativeDAO, MultiHedgingRequestHandler hedger) {
        this.entityRecordDDbNativeDAO = entityRecordDDbNativeDAO;
        this.multiHedgingRequestHandler = hedger;
    }

    @Override
    public List<DDBMetaDataAccessor> getEntityRecords(
            String ccNum,
            String clientId,
            int delayInMillis,
            int numberOfHedgers) {

        validateInput(ccNum, clientId, delayInMillis, numberOfHedgers);

        long startTime = System.currentTimeMillis();
        logger.debug("Starting getEntityRecords request for clientId: {}", clientId);

        //Create a list of delays for each hedger
        List<Integer> delaysInMillisList = new ArrayList<>();
        for (int i = 0; i < numberOfHedgers; i++) {
            delaysInMillisList.add(delayInMillis);
        }

        DDBResponse response = useSimpleHedging ?
                getDdbResponseSimple(ccNum, clientId, delayInMillis) :
                getDdbResponseMulti(ccNum, clientId, delaysInMillisList);

        long endTime = System.currentTimeMillis();
        response.setActualLatency(endTime - startTime);

        logger.debug("Completed getEntityRecords request for clientId: {} in {}ms",
                clientId, response.getActualLatency());

        List<DDBMetaDataAccessor> metaDataAccessor = new ArrayList<>();
        metaDataAccessor.add(response);

        return metaDataAccessor;
    }

    private DDBResponse getDdbResponseSimple(String ccNum, String clientId, int delayInMillis) {
        SimpleHedgingRequestHandler simpleHedgingRequestHandler = new SimpleHedgingRequestHandler();

        return simpleHedgingRequestHandler.hedgeRequest(() ->
                        entityRecordDDbNativeDAO
                                .fetchByRecordIDAndEntityNumberAsync(ccNum, clientId),
                delayInMillis).join();
    }


    private DDBResponse getDdbResponseMulti(
            String ccNum,
            String clientId,
            List<Integer> delaysInMillis) {

        CompletableFuture<DDBResponse> future = multiHedgingRequestHandler.hedgeRequests(
                () -> entityRecordDDbNativeDAO
                        .fetchByRecordIDAndEntityNumberAsync(ccNum, clientId),
                delaysInMillis
        );

        return future.join();
    }

    private void validateInput(String ccNum, String clientId, int delayInMillis, int numberOfHedgers) {
        if (ccNum == null || ccNum.trim().isEmpty()) {
            throw new IllegalArgumentException("ccNum cannot be null or empty");
        }
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("clientId cannot be null or empty");
        }
        if (delayInMillis < 0) {
            throw new IllegalArgumentException("delayInMillis cannot be negative");
        }
        if (numberOfHedgers < 1) {
            throw new IllegalArgumentException("numberOfHedgers must be at least 1");
        }
    }
}
