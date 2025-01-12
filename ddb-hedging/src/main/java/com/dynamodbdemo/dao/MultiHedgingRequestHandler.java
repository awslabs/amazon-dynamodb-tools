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

        CompletableFuture<DDBResponse> firstRequest = CompletableFuture.supplyAsync(() -> {
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

        // Create array to hold all futures using the helper method
        CompletableFuture<DDBResponse>[] allRequests = createFutureArray(numberOfHedges + 1);
        allRequests[0] = firstRequest;

        // Create multiple hedged requests
        for (int i = 0; i < numberOfHedges; i++) {
            final int hedgeNumber = i + 1;
            allRequests[hedgeNumber] = new CompletableFuture<>();

            allRequests[hedgeNumber].completeAsync(() -> {

                    logger.info("Hedging Request #" + hedgeNumber);

                    //Pre-check optimization to see whether any of the prior requests has completed before calling the supplier function.
                    for (CompletableFuture<DDBResponse> request : allRequests) {
                        if (request.isDone()) {
                            try {
                                logger.info("Pre-Check exit: Hedging Request #" + hedgeNumber);
                                return request.get();
                            } catch (InterruptedException | ExecutionException e) {
                                //Continue checkin for other requests ignoring failed requests.
                                logger.info("Bypass failed request. Continue processing...");
                            }
                        }
                    }
                    DDBResponse response = supplier.get();
                    response.setRequestNumber(DDBResponse.FIRST_REQUEST + hedgeNumber );
                    return response;
            }, CompletableFuture.delayedExecutor((long) delayInMillis * hedgeNumber, TimeUnit.MILLISECONDS));
        }



        return CompletableFuture.anyOf(allRequests)
                .thenApply(result -> {
                    cancelIncompleteFutures(allRequests);
                    return (DDBResponse) result;
                });
    }

    @SuppressWarnings("unchecked")
    private static <T> CompletableFuture<T>[] createFutureArray(int size) {
        return new CompletableFuture[size];
    }

    private void cancelIncompleteFutures(CompletableFuture<?>[] futures) {
        for (int i = 0; i < futures.length; i++) {
            try {
                if (futures[i] != null && !futures[i].isDone()) {
                    logger.info("Cancelling: Request #" + i);
                    futures[i].cancel(true);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE,"Cancellation failed: Request #" + i , e);
            }
        }
    }


}
