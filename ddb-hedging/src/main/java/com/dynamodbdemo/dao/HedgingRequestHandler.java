package com.dynamodbdemo.dao;

import com.dynamodbdemo.model.auth.DDBResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface HedgingRequestHandler {
    CompletableFuture<DDBResponse> hedgeRequests(
            Supplier<CompletableFuture<DDBResponse>> supplier,
            List<Integer> delaysInMillis);
}
