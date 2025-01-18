package com.dynamodbdemo.dao;

import com.dynamodbdemo.model.auth.DDBResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class SimpleHedgingRequestHandler {

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(SimpleHedgingRequestHandler.class.getName());

    public CompletableFuture<DDBResponse> hedgeRequest(Supplier<DDBResponse> supplier, int delayInMillis) {

        CompletableFuture<DDBResponse> firstRequest = new CompletableFuture<>();
        CompletableFuture<DDBResponse> hedgedRequest = new CompletableFuture<>();

        firstRequest.completeAsync(() -> {
            logger.info("First Request");
            DDBResponse response = supplier.get();
            response.setRequestNumber(DDBResponse.FIRST_REQUEST);
            return response;
        });


        hedgedRequest.completeAsync(() -> {
            logger.info("Hedging Request");
            if (firstRequest.isDone()) {
                try {
                    logger.info("Pre-Check exit: Hedging Request");
                    return firstRequest.get();
                } catch (InterruptedException | ExecutionException e) {
                    //Continue checkin for other requests ignoring failed requests.
                    logger.info("Bypass failed request. Continue processing...");
                }
            }
            DDBResponse response = supplier.get();
            response.setRequestNumber(DDBResponse.SECOND_REQUEST);
            return response;

        }, CompletableFuture.delayedExecutor(delayInMillis, TimeUnit.MILLISECONDS));



        return CompletableFuture.anyOf(firstRequest, hedgedRequest)
                .thenApply(result -> {
                    if (!firstRequest.isDone()) firstRequest.cancel(true);
                    if (!hedgedRequest.isDone()) hedgedRequest.cancel(true);
                    return (DDBResponse) result;
                });

    }

}
