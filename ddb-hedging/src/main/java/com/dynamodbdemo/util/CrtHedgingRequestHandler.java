package com.dynamodbdemo.util;

import com.dynamodbdemo.model.DDBResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;


//@Component("hedgingRequestHandler")
//@ConditionalOnProperty(name = "aws.dynamodb.use-crt-client", havingValue = "true", matchIfMissing = true)
public class CrtHedgingRequestHandler implements HedgingRequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(CrtHedgingRequestHandler.class);

    private final Executor hedgingThreadPool;
    private final ThreadPoolTaskScheduler hedgingScheduler;

    @Autowired
    public CrtHedgingRequestHandler(
            @Qualifier("hedgingThreadPool") Executor hedgingThreadPool,
            @Qualifier("hedgingScheduler") ThreadPoolTaskScheduler hedgingScheduler) {
        this.hedgingThreadPool = hedgingThreadPool;
        this.hedgingScheduler = hedgingScheduler;
    }

    @Override
    public CompletableFuture<DDBResponse> hedgeRequests(
            Supplier<CompletableFuture<DDBResponse>> supplier,
            List<Float> delaysInMillis, boolean cancelPending) {

        if (delaysInMillis == null || delaysInMillis.isEmpty()) {
            return supplier.get();
        }

        logger.info("Initiating initial request");
        CompletableFuture<DDBResponse> firstRequest = supplier.get()
                .thenApplyAsync(response -> {
                    response.setRequestNumber(DDBResponse.FIRST_REQUEST);
                    return response;
                }, hedgingThreadPool);

        // Create a list to hold all futures (including the first request)
        List<CompletableFuture<DDBResponse>> allRequests = new ArrayList<>();
        allRequests.add(firstRequest);

        // Create a CompletableFuture for final result
        CompletableFuture<DDBResponse> finalResult = new CompletableFuture<>();

        // Atomic reference to track which request succeeded
        final AtomicInteger completedRequestNumber = new AtomicInteger(-1);

        // Set completion handler for first request
        firstRequest.whenCompleteAsync((response, throwable) -> {
            if (throwable == null && !finalResult.isDone() &&
                    completedRequestNumber.compareAndSet(-1, DDBResponse.FIRST_REQUEST)) {
                finalResult.complete(response);

                if (cancelPending) {
                    cancelPendingRequests(allRequests);
                }
            } else if (throwable != null && !finalResult.isDone()) {
                logger.warn("First request failed: {}", throwable.getMessage());
                // Don't complete the final result yet, wait for hedged requests
            }
        }, hedgingThreadPool);

        // Create hedged requests for each delay
        for (int i = 0; i < delaysInMillis.size(); i++) {
            final int requestNumber = i + 2;
            final float delayMillis = delaysInMillis.get(i);

            CompletableFuture<DDBResponse> hedgedRequest = new CompletableFuture<>();
            allRequests.add(hedgedRequest);

            // Calculate the future instant for scheduling
            Instant scheduledTime = Instant.now().plusMillis((long)delayMillis);

            // Schedule with Spring's TaskScheduler using Instant
            hedgingScheduler.schedule(() -> {
                // Don't execute if a request already completed
                if (completedRequestNumber.get() >= 0) {
                    logger.info("Previous request already completed, skipping hedge request#{}", requestNumber);
                    hedgedRequest.complete(null);
                    return;
                }

                logger.info("Initiating hedge request#{}", requestNumber);
                CompletableFuture<DDBResponse> request = supplier.get();
                request
                        .thenApplyAsync(response -> {
                            response.setRequestNumber(requestNumber);
                            return response;
                        }, hedgingThreadPool)
                        .whenCompleteAsync((response, throwable) -> {
                            if (throwable == null && response != null && !finalResult.isDone() &&
                                    completedRequestNumber.compareAndSet(-1, requestNumber)) {
                                finalResult.complete(response);

                                if (cancelPending) {
                                    cancelPendingRequests(allRequests);
                                }
                            } else if (throwable != null) {
                                logger.warn("Hedged request#{} failed: {}", requestNumber, throwable.getMessage());
                            }

                            // Complete the hedgedRequest future
                            if (throwable == null) {
                                hedgedRequest.complete(response);
                            } else {
                                hedgedRequest.completeExceptionally(throwable);
                            }
                        }, hedgingThreadPool);
            }, scheduledTime);
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