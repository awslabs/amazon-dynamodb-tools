package com.dynamodbdemo.util;

import com.dynamodbdemo.model.DDBResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class CrtHedgingRequestHandler implements HedgingRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(CrtHedgingRequestHandler.class);

    public CompletableFuture<DDBResponse> hedgeRequests(
            Supplier<CompletableFuture<DDBResponse>> supplier,
            List<Float> delaysInMillis, boolean cancelPending) {

        if (delaysInMillis == null || delaysInMillis.isEmpty()) {
            return supplier.get();
        }

        logger.info("Initiating initial request");
        CompletableFuture<DDBResponse> firstRequest = supplier.get()
                .thenApply(response -> {
                    response.setRequestNumber(DDBResponse.FIRST_REQUEST);
                    return response;
                });

        // Create a list to hold all futures (including the first request)
        List<CompletableFuture<DDBResponse>> allRequests = new ArrayList<>();
        allRequests.add(firstRequest);

        // Create a CompletableFuture for final result
        CompletableFuture<DDBResponse> finalResult = new CompletableFuture<>();

        // Atomic reference to track which request succeeded
        final AtomicInteger completedRequestNumber = new AtomicInteger(-1);

        // Set completion handler for first request
        firstRequest.whenComplete((response, throwable) -> {
            if (throwable == null && !finalResult.isDone() &&
                    completedRequestNumber.compareAndSet(-1, DDBResponse.FIRST_REQUEST)) {
                finalResult.complete(response);

                if (cancelPending) {
                    cancelPendingRequests(allRequests);
                }
            }
        });

        // Create hedged requests for each delay
        for (int i = 0; i < delaysInMillis.size(); i++) {
            final int requestNumber = i + 2;
            long delay = (long)(delaysInMillis.get(i) * 1_000_000L);

            CompletableFuture<DDBResponse> hedgedRequest = CompletableFuture.supplyAsync(() -> {
                // Don't execute if a request already completed
                if (completedRequestNumber.get() >= 0) {
                    logger.info("Previous request already completed, skipping hedge request#{}", requestNumber);
                    return null;
                }

                logger.info("Initiating hedge request#{}", requestNumber);
                return supplier.get()
                        .thenApply(response -> {
                            response.setRequestNumber(requestNumber);
                            return response;
                        })
                        .exceptionally(throwable -> {
                            logger.warn("Hedged request#{} failed: {}", requestNumber, throwable.getMessage());
                            return null;
                        })
                        .join();
            }, CompletableFuture.delayedExecutor(delay, TimeUnit.NANOSECONDS));

            allRequests.add(hedgedRequest);

            // Set completion handler for this hedged request
            hedgedRequest.whenComplete((response, throwable) -> {
                if (throwable == null && response != null && !finalResult.isDone() &&
                        completedRequestNumber.compareAndSet(-1, requestNumber)) {
                    finalResult.complete(response);

                    if (cancelPending) {
                        cancelPendingRequests(allRequests);
                    }
                }
            });
        }

        return finalResult;
    }

    private void cancelPendingRequests(List<CompletableFuture<DDBResponse>> allRequests) {
        logger.info("Cancelling pending requests");
        allRequests.forEach(request -> {
            if (!request.isDone() && !request.isCancelled()) {
                request.cancel(true);
            }
        });
    }
}