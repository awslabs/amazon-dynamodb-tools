package com.dynamodbdemo.dao;

import com.dynamodbdemo.model.auth.DDBResponse;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class NettyHedgingRequestHandler implements HedgingRequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(NettyHedgingRequestHandler.class);
    private static final int MAX_HEDGED_REQUESTS = 5;

    private final EventLoopGroup eventLoopGroup;

    // Constructor that accepts an existing EventLoopGroup
    public NettyHedgingRequestHandler(EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
    }


    @Override
    public CompletableFuture<DDBResponse> hedgeRequests(
            Supplier<CompletableFuture<DDBResponse>> supplier,
            List<Integer> delaysInMillis) {

        if (delaysInMillis == null || delaysInMillis.isEmpty()) {
            throw new IllegalArgumentException("Delays list cannot be null or empty");
        }

        if (delaysInMillis.size() > MAX_HEDGED_REQUESTS) {
            throw new IllegalArgumentException("Number of hedged requests cannot exceed " + MAX_HEDGED_REQUESTS);
        }

        List<CompletableFuture<DDBResponse>> futures = new ArrayList<>();
        EventLoop eventLoop = getEventLoop(); // Get the current Netty EventLoop

        // Initial request
        logger.info("Initiating request");
        CompletableFuture<DDBResponse> initialRequest = supplier.get()
                .thenApply(response -> {
                    logger.info("Initial request completed");
                    response.setRequestNumber(DDBResponse.FIRST_REQUEST);
                    return response;
                });
        futures.add(initialRequest);

        // Create hedged requests
        for (int i = 0; i < delaysInMillis.size(); i++) {
            final int requestNumber = i + 2;
            int delay = delaysInMillis.get(i);

            CompletableFuture<DDBResponse> hedgedRequest = new CompletableFuture<>();

            // Schedule the hedged request using Netty's EventLoop
            eventLoop.schedule(() -> {
                // Check if any previous request is complete
                if (futures.stream().anyMatch(CompletableFuture::isDone)) {
                    logger.info("Previous request already completed, skipping hedge request {}", requestNumber);
                    Optional<CompletableFuture<DDBResponse>> completedFuture = futures.stream()
                            .filter(CompletableFuture::isDone)
                            .findFirst();

                    if (completedFuture.isPresent()) {
                        hedgedRequest.complete(completedFuture.get().join());
                        return;
                    }
                }


                logger.info("Initiating hedge request#{}", requestNumber);
                supplier.get()
                        .thenAccept(response -> {
                            response.setRequestNumber(requestNumber);
                            hedgedRequest.complete(response);
                        })
                        .exceptionally(throwable -> {
                            if (!(throwable instanceof CancellationException)) {
                                logger.warn("Hedged request#{} failed: {}", requestNumber, throwable.getMessage());
                                hedgedRequest.completeExceptionally(throwable);
                            }
                            return null;
                        });
            }, delay, TimeUnit.MILLISECONDS);

            futures.add(hedgedRequest);
        }

        // Return the first successful response
        return CompletableFuture.anyOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(result -> {
                    cancelPendingRequests(futures, ((DDBResponse) result).getRequestNumber());
                    return (DDBResponse) result;
                })
                .exceptionally(throwable -> {
                    logger.error("All requests failed", throwable);
                    throw new RuntimeException("All hedged requests failed", throwable);
                });
    }

    private void cancelPendingRequests(List<CompletableFuture<DDBResponse>> futures, int completedRequestNumber) {
        logger.info("Request {} completed, cancelling other pending requests", completedRequestNumber);
        futures.forEach(future -> {
            if (!future.isDone()) {
                future.cancel(true);
            }
        });
    }


    private EventLoop getEventLoop() {
        return eventLoopGroup.next();
    }
}
