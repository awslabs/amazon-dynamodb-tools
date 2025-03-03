package com.dynamodbdemo.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class ThreadPoolMonitoringConfig {

    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolMonitoringConfig.class);

    @Value("${threadpool.monitor.period:1}")
    int period = 1;

    @Bean
    public ScheduledExecutorService threadPoolMonitor(
            @Qualifier("hedgingThreadPool") ThreadPoolTaskExecutor executor,
            @Qualifier("hedgingScheduler") ThreadPoolTaskScheduler scheduler) {

        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();

        monitor.scheduleAtFixedRate(() -> {
            // Monitor the executor
            ThreadPoolExecutor threadPoolExecutor = executor.getThreadPoolExecutor();

            logger.info("HedgingThreadPool Metrics: " +
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

            // Monitor the scheduler - extract the underlying ScheduledThreadPoolExecutor
            ScheduledThreadPoolExecutor schedulerExecutor =
                    (ScheduledThreadPoolExecutor) scheduler.getScheduledExecutor();

            logger.info("HedgingScheduler Metrics: " +
                            " Active Threads: {}" +
                            " Core Threads: {}" +
                            " Pool Size: {}" +
                            " Queue Size: {}" +
                            " Task Count: {}" +
                            " Completed Tasks: {}" +
                            " Scheduled Task Count: {}",
                    schedulerExecutor.getActiveCount(),
                    schedulerExecutor.getCorePoolSize(),
                    schedulerExecutor.getPoolSize(),
                    schedulerExecutor.getQueue().size(),
                    schedulerExecutor.getTaskCount(),
                    schedulerExecutor.getCompletedTaskCount(),
                    schedulerExecutor.getQueue().size() // Approximation of scheduled tasks
            );

        }, 0, period, TimeUnit.MINUTES);

        return monitor;
    }

    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService monitorCleanupService() {
        return Executors.newSingleThreadScheduledExecutor();
    }
}