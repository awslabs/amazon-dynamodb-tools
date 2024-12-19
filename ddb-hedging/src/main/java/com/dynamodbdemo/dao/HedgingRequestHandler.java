package com.dynamodbdemo.dao;

import com.dynamodbdemo.model.auth.DDBResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class HedgingRequestHandler {

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(HedgingRequestHandler.class.getName());

    public CompletableFuture<DDBResponse> hedgeRequest(Supplier<DDBResponse> supplier, int delayInMillis) {

        CompletableFuture<DDBResponse> firstRequest = new CompletableFuture<>();
        CompletableFuture<DDBResponse> hedgedRequest = new CompletableFuture<>();

        firstRequest.completeAsync(() -> {
            logger.info("First Request: " + Thread.currentThread());
            DDBResponse response = supplier.get();
            response.setRequestNumber(DDBResponse.FIRST_REQUEST);
            return response;
        });


        hedgedRequest.completeAsync(() -> {
            logger.info("Hedging Request: " + Thread.currentThread());
            if (firstRequest.isDone()) {
                //Don't do anything if the first request has processed.
                return null;
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
