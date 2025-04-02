package com.dynamodbdemo.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class GenericHedgingRequestHandler<T> {

    private static final Logger logger = LoggerFactory.getLogger(GenericHedgingRequestHandler.class);

    public CompletableFuture<T> hedgeRequests(
            Supplier<CompletableFuture<T>> supplier,
            List<Float> delaysInMillis,
            boolean cancelPending) {

        if (delaysInMillis == null || delaysInMillis.isEmpty()) {
            return supplier.get();
        }

        logger.info("Initiating initial request");
        CompletableFuture<T> firstRequest = supplier.get()
                .thenApply(response -> response);

        List<CompletableFuture<T>> allRequests = new ArrayList<>();
        allRequests.add(firstRequest);

        // Create hedged requests for each delay
        for (int i = 0; i < delaysInMillis.size(); i++) {
            final int requestNumber = i + 2;
            long delay = (long)((double)delaysInMillis.get(i) * 1_000_000L);

            CompletableFuture<T> hedgedRequest = CompletableFuture.supplyAsync(() -> {
                logger.info("Check: Before hedged request#{} can be initiated", requestNumber);
                CompletableFuture<T> completedFuture = allRequests.stream()
                        .filter(CompletableFuture::isDone)
                        .findFirst()
                        .orElse(null);

                if (completedFuture != null) {
                    logger.info("Previous request already completed, skipping hedge request#{}", requestNumber);
                    return completedFuture.join();
                }

                logger.info("Initiating hedge request#{}", requestNumber);
                return supplier.get()
                        .thenApply(response -> {
                            return response;
                        })
                        .exceptionally(throwable -> {
                            logger.warn("Hedged request#{} failed: {}", requestNumber, throwable.getMessage());
                            return firstRequest.join();
                        })
                        .join();
            }, CompletableFuture.delayedExecutor(delay, TimeUnit.NANOSECONDS));

            allRequests.add(hedgedRequest);
        }

        return CompletableFuture.anyOf(allRequests.toArray(new CompletableFuture[0]))
                .thenApply(result -> {
                    @SuppressWarnings("unchecked")
                    T response = (T) result;
                    if (cancelPending) {
                        cancelPendingRequests(allRequests);
                    }
                    return response;
                });
    }

    private void cancelPendingRequests(List<CompletableFuture<T>> allRequests) {
        logger.info("Cancelling pending requests");
        allRequests.forEach(request -> {
            if (!request.isDone()) {
                request.cancel(true);
            }
        });
    }
}