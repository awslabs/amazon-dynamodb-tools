package com.dynamodbdemo.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class ThreadPoolMonitoringConfig {

    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolMonitoringConfig.class);

    @Value("${threadpool.monitor.period:1}")
    int period = 1;

    @Bean
    public ScheduledExecutorService threadPoolMonitor(ThreadPoolTaskExecutor executor) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();



        scheduler.scheduleAtFixedRate(() -> {
            ThreadPoolExecutor threadPoolExecutor = executor.getThreadPoolExecutor();

            logger.info("Thread Pool Metrics: " +
                            " Active Threads: {}" +
                            " Core Threads: {}" +
                            " Pool Size: {}" +
                            " Max Pool Size: {}" +
                            " Queue Size: {}" +
                            " Task Count: {}" +
                            " Completed Tasks: {}" +
                            " Queue Remaining Capacity: {}",
                    threadPoolExecutor.getActiveCount(),
                    threadPoolExecutor.getCorePoolSize(),
                    threadPoolExecutor.getPoolSize(),
                    threadPoolExecutor.getMaximumPoolSize(),
                    threadPoolExecutor.getQueue().size(),
                    threadPoolExecutor.getTaskCount(),
                    threadPoolExecutor.getCompletedTaskCount(),
                    threadPoolExecutor.getQueue().remainingCapacity()
            );
        }, 0, period, TimeUnit.MINUTES);

        return scheduler;
    }
}
