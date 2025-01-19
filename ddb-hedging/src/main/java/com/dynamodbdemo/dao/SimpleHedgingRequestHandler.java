package com.dynamodbdemo.dao;

import com.dynamodbdemo.model.auth.DDBResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class SimpleHedgingRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(SimpleHedgingRequestHandler.class);

    public CompletableFuture<DDBResponse> hedgeRequest(
            Supplier<CompletableFuture<DDBResponse>> supplier,
            int delayInMillis) {

        logger.info("Initiating request");
        CompletableFuture<DDBResponse> firstRequest = supplier.get()
                .thenApply(response -> {
                    response.setRequestNumber(DDBResponse.FIRST_REQUEST);
                    return response;
                });

        return CompletableFuture.supplyAsync(() -> {

                    // Check if first request is already complete
                    if (firstRequest.isDone()) {
                        logger.info("First request already completed, skipping hedge request");
                        return firstRequest.join();
                    }

                    // If first request isn't complete, make hedged request
                    logger.info("Initiating hedge request#{}", DDBResponse.SECOND_REQUEST);
                    return supplier.get()
                            .thenApply(response -> {
                                response.setRequestNumber(DDBResponse.SECOND_REQUEST);
                                return response;
                            })
                            .exceptionally(throwable -> {
                                logger.warn("Hedged request failed: {}", throwable.getMessage());
                                // If hedged request fails, wait for first request
                                return firstRequest.join();
                            })
                            .join();
                },  CompletableFuture.delayedExecutor(delayInMillis, TimeUnit.MILLISECONDS))
                // Return whichever request completes first
                .applyToEither(firstRequest, response -> response);
    }
}
