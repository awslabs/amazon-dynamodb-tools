package com.dynamodbdemo.service.Impl;

import com.dynamodbdemo.dao.EntityRecordDDbNativeDAO;
import com.dynamodbdemo.model.DDBMetaDataAccessor;
import com.dynamodbdemo.model.DDBResponse;
import com.dynamodbdemo.util.HedgingRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Service("EntityRecordReadServiceDDbNativeHedgingImpl")
public class EntityRecordReadServiceDDbNativeHedgingImpl extends AbstractEntityRecordReadServiceImpl {

    private static final Logger logger = LoggerFactory.getLogger(EntityRecordReadServiceDDbNativeHedgingImpl.class);

    private final EntityRecordDDbNativeDAO entityRecordDDbNativeDAO;
    private final HedgingRequestHandler hedgingRequestHandler;

    @Value("${ddb.hedging.cancelPending}")
    private boolean cancelPending;

    public EntityRecordReadServiceDDbNativeHedgingImpl(
            EntityRecordDDbNativeDAO entityRecordDDbNativeDAO,
            @Qualifier("hedgingRequestHandler") HedgingRequestHandler hedgingRequestHandler) {
        this.entityRecordDDbNativeDAO = entityRecordDDbNativeDAO;
        this.hedgingRequestHandler = hedgingRequestHandler;
    }

    @Override
    public List<DDBMetaDataAccessor> getEntityRecords(
            String ccNum, String clientId, float delayInMillis, int numberOfHedgers) {

        validateInput(ccNum, clientId, delayInMillis, numberOfHedgers);

        long startTime = System.nanoTime();
        logger.debug("Starting getEntityRecords request for clientId: {}", clientId);

        // Create a list of delays for each hedger
        List<Float> delaysInMillisList = createDelaysList(delayInMillis, numberOfHedgers);

        try {
            DDBResponse response = getDdbResponse(ccNum, clientId, delaysInMillisList);

            long endTime = System.nanoTime();
            response.setActualLatency(endTime - startTime);

            logger.debug("Completed getEntityRecords request for clientId: {} in {}ms",
                    clientId, response.getActualLatency());

            List<DDBMetaDataAccessor> metaDataAccessor = new ArrayList<>();
            metaDataAccessor.add(response);

            return metaDataAccessor;
        } catch (Exception e) {
            logger.error("Error in getEntityRecords for clientId: {}", clientId, e);
            throw e;
        }
    }

    @Override
    public CompletableFuture<List<DDBMetaDataAccessor>> getEntityRecordsAsync(
            String ccNum, String clientId, float delayInMillis, int numberOfHedgers) {

        try {
            validateInput(ccNum, clientId, delayInMillis, numberOfHedgers);

            long startTime = System.nanoTime();
            logger.debug("Starting async getEntityRecords request for clientId: {}", clientId);

            List<Float> delaysInMillisList = createDelaysList(delayInMillis, numberOfHedgers);

            return getDdbResponseAsync(ccNum, clientId, delaysInMillisList)
                    .thenApply(response -> {
                        long endTime = System.nanoTime();
                        response.setActualLatency(endTime - startTime);

                        logger.debug("Completed async getEntityRecords for clientId: {} in {}ms",
                                clientId, response.getActualLatency());

                        List<DDBMetaDataAccessor> metaDataAccessor = new ArrayList<>();
                        metaDataAccessor.add(response);
                        return metaDataAccessor;
                    })
                    .exceptionally(throwable -> {
                        logger.error("Error in async getEntityRecords for clientId: {}", clientId, throwable);
                        throw new CompletionException(throwable);
                    });
        } catch (Exception e) {
            logger.error("Error in getEntityRecordsAsync setup for clientId: {}", clientId, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    private List<Float> createDelaysList(float delayInMillis, int numberOfHedgers) {
        List<Float> delaysInMillisList = new ArrayList<>(numberOfHedgers);
        for (int i = 0; i < numberOfHedgers; i++) {
            delaysInMillisList.add(delayInMillis);
        }
        return delaysInMillisList;
    }

    private DDBResponse getDdbResponse(
            String ccNum, String clientId, List<Float> delaysInMillis) {

        return hedgingRequestHandler.hedgeRequests(
                () -> entityRecordDDbNativeDAO.fetchByRecordIDAndEntityNumberAsync(ccNum, clientId),
                delaysInMillis, cancelPending
        ).join();
    }

    private CompletableFuture<DDBResponse> getDdbResponseAsync(
            String ccNum, String clientId, List<Float> delaysInMillis) {

        long hedgingStartTime = System.nanoTime();

        return hedgingRequestHandler.hedgeRequests(
                () -> entityRecordDDbNativeDAO.fetchByRecordIDAndEntityNumberAsync(ccNum, clientId),
                delaysInMillis, cancelPending
        ).thenApply(response -> {
            long hedgingEndTime = System.nanoTime();
            response.setActualLatency(hedgingEndTime - hedgingStartTime);
            return response;
        });
    }

    private void validateInput(String ccNum, String clientId, float delayInMillis, int numberOfHedgers) {
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
