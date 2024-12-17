package com.dynamodbdemo.dao;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;

public class HedgingRequestHandler<T> {

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(HedgingRequestHandler.class.getName());

    public CompletableFuture<T> hedgeRequest(Supplier<T> supplier, int delayInMillis) {

        logger.log(Level.FINE, "hedgingRequest - start " + Thread.currentThread());

        CompletableFuture<T> firstRequest = new CompletableFuture<>();
        CompletableFuture<T> hedgedRequest = new CompletableFuture<>();

        firstRequest.completeAsync(() -> {
            logger.log(Level.FINE, "hedgingRequest - Inside First - Start " + Thread.currentThread());
            T response = supplier.get();
            logger.log(Level.FINE, "hedgingRequest - First request - End.");
            return response;
        });


        hedgedRequest.completeAsync(() -> {

            logger.log(Level.FINE, "hedgingRequest - second request - start " + Thread.currentThread());

            if (firstRequest.isDone()) {
                //Don't do anything if the first request has processed.
                return null;
            }


            T Response = supplier.get();
            logger.log(Level.FINE, "hedgingRequest - second request - end");
            return Response;

        }, CompletableFuture.delayedExecutor(delayInMillis, TimeUnit.MILLISECONDS));


        return CompletableFuture.anyOf(firstRequest, hedgedRequest)
                .thenApply(result -> {
                    if (!firstRequest.isDone()) firstRequest.cancel(true);
                    if (!hedgedRequest.isDone()) hedgedRequest.cancel(true);
                    return (T) result;
                });

    }

}
