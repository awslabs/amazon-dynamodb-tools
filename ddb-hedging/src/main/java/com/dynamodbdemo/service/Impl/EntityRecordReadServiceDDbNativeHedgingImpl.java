package com.dynamodbdemo.service.Impl;

import com.dynamodbdemo.dao.EntityRecordDDbNativeDAO;
import com.dynamodbdemo.util.HedgingRequestHandler;
import com.dynamodbdemo.model.DDBMetaDataAccessor;
import com.dynamodbdemo.model.DDBResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service("EntityRecordReadServiceDDbNativeHedgingImpl")
@Configuration
public class EntityRecordReadServiceDDbNativeHedgingImpl extends AbstractEntityRecordReadServiceImpl {

    private static final Logger logger = LoggerFactory.getLogger(EntityRecordReadServiceDDbNativeHedgingImpl.class);

    private final EntityRecordDDbNativeDAO entityRecordDDbNativeDAO;

    private final HedgingRequestHandler hedgingRequestHandler;

    public EntityRecordReadServiceDDbNativeHedgingImpl(
            EntityRecordDDbNativeDAO entityRecordDDbNativeDAO, @Qualifier("hedgingRequestHandler") HedgingRequestHandler hedgingRequestHandler) {
        this.entityRecordDDbNativeDAO = entityRecordDDbNativeDAO;
        this.hedgingRequestHandler = hedgingRequestHandler;
    }

    @Override
    public List<DDBMetaDataAccessor> getEntityRecords(
            String ccNum,
            String clientId,
            int delayInMillis,
            int numberOfHedgers) {

        validateInput(ccNum, clientId, delayInMillis, numberOfHedgers);

        long startTime = System.nanoTime();
        logger.debug("Starting getEntityRecords request for clientId: {}", clientId);

        //Create a list of delays for each hedger
        List<Integer> delaysInMillisList = new ArrayList<>();
        for (int i = 0; i < numberOfHedgers; i++) {
            delaysInMillisList.add(delayInMillis);
        }

        DDBResponse response = getDdbResponse(ccNum, clientId, delaysInMillisList);

        long endTime = System.nanoTime();
        response.setActualLatency(endTime - startTime);

        logger.debug("Completed getEntityRecords request for clientId: {} in {}ms",
                clientId, response.getActualLatency());

        List<DDBMetaDataAccessor> metaDataAccessor = new ArrayList<>();
        metaDataAccessor.add(response);

        return metaDataAccessor;
    }


    private DDBResponse getDdbResponse(
            String ccNum,
            String clientId,
            List<Integer> delaysInMillis) {

        CompletableFuture<DDBResponse> future = hedgingRequestHandler.hedgeRequests(
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
