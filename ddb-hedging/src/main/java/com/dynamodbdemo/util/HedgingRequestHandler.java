package com.dynamodbdemo.util;

import com.dynamodbdemo.model.DDBResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface HedgingRequestHandler {
    CompletableFuture<DDBResponse> hedgeRequests(
            Supplier<CompletableFuture<DDBResponse>> supplier,
            List<Float> delaysInMillis);
}
