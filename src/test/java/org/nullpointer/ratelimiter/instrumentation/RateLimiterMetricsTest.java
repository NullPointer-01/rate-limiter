package org.nullpointer.ratelimiter.instrumentation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RateLimiterMetricsTest {

    @Test
    void returnsNaNWhenNoRequestsAreRecorded() {
        RateLimiterMetrics metrics = new RateLimiterMetrics();

        assertTrue(Double.isNaN(metrics.getRejectionRate()));
    }

    @Test
    void computesRejectionRateFromAllowedAndRejectedRequests() {
        RateLimiterMetrics metrics = new RateLimiterMetrics();

        metrics.logAllowed();
        metrics.logAllowed();
        metrics.logRejected();

        assertEquals(33.3333, metrics.getRejectionRate(), 0.0001);
    }

    @Test
    void returnsHundredPercentWhenAllRequestsAreRejected() {
        RateLimiterMetrics metrics = new RateLimiterMetrics();

        metrics.logRejected();
        metrics.logRejected();

        assertEquals(100.0, metrics.getRejectionRate(), 0.0);
    }

    @Test
    void maintainsCorrectRejectionRateUnderConcurrentUpdates() throws InterruptedException {
        RateLimiterMetrics metrics = new RateLimiterMetrics();
        int threads = 8;
        int iterationsPerThread = 2000;

        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        List<Runnable> tasks = new ArrayList<>();

        for (int index = 0; index < threads; index++) {
            final boolean logRejected = index % 2 == 0; // Allow even threads to log rejected, odd threads to log allowed
            tasks.add(() -> {
                for (int iteration = 0; iteration < iterationsPerThread; iteration++) {
                    if (logRejected) {
                        metrics.logRejected();
                    } else {
                        metrics.logAllowed();
                    }
                }
            });
        }

        for (Runnable task : tasks) {
            executorService.execute(task);
        }

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(50.0, metrics.getRejectionRate(), 0.0);
    }
}