package com.dynamodbdemo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class AsyncConfiguration {

    @Bean(name = "hedgingThreadPool")
    public ThreadPoolTaskExecutor hedgingThreadPool() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // For IO-bound tasks like DynamoDB hedging requests
        int cores = Runtime.getRuntime().availableProcessors();
        executor.setCorePoolSize(cores * 2);
        executor.setMaxPoolSize(cores * 4);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("Hedging-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setKeepAliveSeconds(60);
        executor.initialize();

        return executor;
    }

    @Bean(name = "hedgingScheduler")
    public ThreadPoolTaskScheduler hedgingScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(Runtime.getRuntime().availableProcessors());
        scheduler.setThreadNamePrefix("HedgingScheduler-");
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        scheduler.initialize();
        return scheduler;
    }
}