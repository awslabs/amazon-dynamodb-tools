package com.dynamodbdemo.dao;

import com.dynamodbdemo.model.auth.DDBResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;

public class MultiHedgingRequestHandler {

    private static final java.util.logging.Logger logger = java.util.logging.Logger.getLogger(MultiHedgingRequestHandler.class.getName());

    public CompletableFuture<DDBResponse> hedgeRequest(Supplier<DDBResponse> supplier, int delayInMillis, int numberOfHedges) {
        if (numberOfHedges < 1) {
            throw new IllegalArgumentException("Number of hedges must be at least 1");
        }

        CompletableFuture<DDBResponse> firstRequest = new CompletableFuture<>();
        CompletableFuture<DDBResponse>[] hedgedRequests = new CompletableFuture[numberOfHedges];

        // Initialize first request
        firstRequest.completeAsync(() -> {
            try {
                logger.info("First Request");
                DDBResponse response = supplier.get();
                response.setRequestNumber(DDBResponse.FIRST_REQUEST);
                return response;
            } catch (Exception e) {
                logger.log(Level.SEVERE,"Error in first request", e);
                throw e;
            }
        });

        // Create multiple hedged requests
        for (int i = 0; i < numberOfHedges; i++) {
            final int hedgeNumber = i + 1;
            hedgedRequests[i] = new CompletableFuture<>();

            hedgedRequests[i].completeAsync(() -> {

                    logger.info("Hedging Request #" + hedgeNumber);
                    if (firstRequest.isDone()) {
                        try {
                            return firstRequest.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    DDBResponse response = supplier.get();
                    response.setRequestNumber(DDBResponse.FIRST_REQUEST + hedgeNumber );
                    return response;
            }, CompletableFuture.delayedExecutor((long) delayInMillis * hedgeNumber, TimeUnit.MILLISECONDS));
        }

        // Combine all futures into a single future that completes when any of them complete
        CompletableFuture<DDBResponse>[] allRequests = new CompletableFuture[numberOfHedges + 1];
        allRequests[0] = firstRequest;
        System.arraycopy(hedgedRequests, 0, allRequests, 1, numberOfHedges);

        return CompletableFuture.anyOf(allRequests)
                .thenApply(result -> {
                    try {
                        // Cancel all incomplete futures
                        cancelIncompleteFutures(allRequests);
                    } catch (Exception e) {
                        logger.log(Level.SEVERE,"Cancellation failed: ", e);
                    }
                    return (DDBResponse) result;
                });
    }

    private void cancelIncompleteFutures(CompletableFuture<?>[] futures) {
        for (CompletableFuture<?> future : futures) {
            if (future != null && !future.isDone()) {
                future.cancel(true);
            }
        }
    }


}
