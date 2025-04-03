package com.dynamodbdemo.util;

import com.dynamodbdemo.model.DDBResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Component("hedgingRequestHandler")
@ConditionalOnProperty(name = "aws.dynamodb.use-crt-client", havingValue = "true", matchIfMissing = true)
public class HedgingRequestHandler2 implements HedgingRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(CrtHedgingRequestHandler.class);

    public CompletableFuture<DDBResponse> hedgeRequests(
            Supplier<CompletableFuture<DDBResponse>> supplier,
            List<Float> delaysInMillis, boolean cancelPending)
    {

        if (delaysInMillis == null || delaysInMillis.isEmpty()) {
            return supplier.get();
        }

        logger.info("Initiating initial request");
        CompletableFuture<DDBResponse> firstRequest = supplier.get()
                .thenApply(response -> {
                    response.setRequestNumber(DDBResponse.FIRST_REQUEST); // First request is number 0
                    return response;
                });

        // Create a list to hold all futures (including the first request)
        List<CompletableFuture<DDBResponse>> allRequests = new ArrayList<>();
        allRequests.add(firstRequest);

        // Create hedged requests for each delay
        for (int i = 0; i < delaysInMillis.size(); i++) {
            final int requestNumber = i + 2;

            //Convert to Nano Seconds
            long delay = (long)((double)delaysInMillis.get(i) * 1_000_000L);

            CompletableFuture<DDBResponse> hedgedRequest = CompletableFuture.supplyAsync(() -> {
                logger.info("Check Before hedged request#{} can be initiated", requestNumber);
                // Check if any previous request is already complete
                CompletableFuture<DDBResponse> completedFuture = allRequests.stream()
                        .filter(CompletableFuture::isDone)
                        .findFirst()
                        .orElse(null);

                if (completedFuture != null) {
                    logger.info("Previous request already completed, skipping hedge request#{}", requestNumber);
                    return completedFuture.join();
                    //throw new CancellationException("Previous request already completed");
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
            }, CompletableFuture.delayedExecutor(delay, TimeUnit.NANOSECONDS));

            allRequests.add(hedgedRequest);
        }

        // Return the result of whichever request completes first and cancel others
        return CompletableFuture.anyOf(allRequests.toArray(new CompletableFuture[0]))
                .thenApply(result -> {
                    DDBResponse ddbResponse = (DDBResponse) result;
                    // Cancel all pending requests
                    if (cancelPending) {
                        cancelPendingRequests(allRequests, ddbResponse.getRequestNumber());
                    }
                    return ddbResponse;
                });
    }

    private void cancelPendingRequests(List<CompletableFuture<DDBResponse>> allRequests, int requestNumber) {
        logger.info("Request {} completed, cancelling other pending requests", requestNumber);
        allRequests.forEach(request -> {
            if (!request.isDone()) {
                request.cancel(true);
            }
        });
    }

}
