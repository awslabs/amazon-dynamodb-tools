package com.dynamodbdemo.dao;

import com.dynamodbdemo.model.auth.DDBResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class CrtHedgingRequestHandler implements HedgingRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(CrtHedgingRequestHandler.class);

    public CompletableFuture<DDBResponse> hedgeRequests(
            Supplier<CompletableFuture<DDBResponse>> supplier,
            List<Integer> delaysInMillis) {

        if (delaysInMillis == null || delaysInMillis.isEmpty()) {
            return supplier.get();
        }

        logger.info("Initiating initial request");
        CompletableFuture<DDBResponse> firstRequest = supplier.get()
                .thenApply(response -> {
                    response.setRequestNumber(0); // First request is number 0
                    return response;
                });

        // Create a list to hold all futures (including the first request)
        List<CompletableFuture<DDBResponse>> allRequests = new ArrayList<>();
        allRequests.add(firstRequest);

        // Create hedged requests for each delay
        for (int i = 0; i < delaysInMillis.size(); i++) {
            final int requestNumber = i + 1;
            int delay = delaysInMillis.get(i);

            CompletableFuture<DDBResponse> hedgedRequest = CompletableFuture.supplyAsync(() -> {
                // Check if any previous request is already complete
                CompletableFuture<DDBResponse> completedFuture = allRequests.stream()
                        .filter(CompletableFuture::isDone)
                        .findFirst()
                        .orElse(null);

                if (completedFuture != null) {
                    logger.info("Previous request already completed, skipping hedge request#{}", requestNumber);
                    return completedFuture.join();
                }

                // If no previous request is complete, make new hedged request
                logger.info("Initiating hedge request#{}", requestNumber);
                return supplier.get()
                        .thenApply(response -> {
                            response.setRequestNumber(requestNumber);
                            return response;
                        })
                        .exceptionally(throwable -> {
                            logger.warn("Hedged request#{} failed: {}", requestNumber, throwable.getMessage());
                            // If hedged request fails, wait for first request
                            return firstRequest.join();
                        })
                        .join();
            }, CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS));

            allRequests.add(hedgedRequest);
        }

        // Return the result of whichever request completes first and cancel others
        return CompletableFuture.anyOf(allRequests.toArray(new CompletableFuture[0]))
                .thenApply(result -> {
                    // Cancel all pending requests
                    allRequests.forEach(request -> {
                        if (!request.isDone()) {
                            request.cancel(true);
                            logger.info("Cancelled pending request");
                        }
                    });
                    return (DDBResponse) result;
                });
    }

}
