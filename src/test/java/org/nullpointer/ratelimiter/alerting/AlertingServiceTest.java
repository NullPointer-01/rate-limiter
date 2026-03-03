package org.nullpointer.ratelimiter.alerting;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.instrumentation.RateLimiterMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlertingServiceTest {
    private static final Logger LOGGER = Logger.getLogger(AlertingService.class.getName());

    private TestLogHandler handler;

    @BeforeEach
    void setupLogger() {
        handler = new TestLogHandler();
        LOGGER.setUseParentHandlers(false);
        LOGGER.addHandler(handler);
    }

    @AfterEach
    void teardownLogger() {
        LOGGER.removeHandler(handler);
    }

    @Test
    void monitorDoesNotSendAlertWhenRejectionRateIsBelowThreshold() {
        RateLimiterMetrics metrics = new RateLimiterMetrics();
        AlertingService alertingService = new AlertingService(metrics);

        metrics.logAllowed();
        metrics.logAllowed();
        metrics.logAllowed();
        metrics.logAllowed();
        metrics.logAllowed();
        metrics.logRejected();

        alertingService.monitor();

        assertTrue(handler.messages.isEmpty());
    }

    @Test
    void monitorSendsAlertWhenRejectionRateReachesThreshold() {
        RateLimiterMetrics metrics = new RateLimiterMetrics();
        AlertingService alertingService = new AlertingService(metrics);

        metrics.logAllowed();
        metrics.logAllowed();
        metrics.logAllowed();
        metrics.logAllowed();
        metrics.logRejected();

        alertingService.monitor();

        assertEquals(1, handler.messages.size());
        assertTrue(handler.messages.get(0).contains("ThresholdRejectionReachedAlert{rejectionRate=20.0}"));
    }

    @Test
    void monitorSendsAlertWhenRejectionRateIsAboveThreshold() {
        RateLimiterMetrics metrics = new RateLimiterMetrics();
        AlertingService alertingService = new AlertingService(metrics);

        metrics.logRejected();

        alertingService.monitor();

        assertEquals(1, handler.messages.size());
        assertTrue(handler.messages.get(0).contains("ThresholdRejectionReachedAlert{rejectionRate=100.0}"));
    }

    private static final class TestLogHandler extends Handler {
        private final List<String> messages = new ArrayList<>();

        @Override
        public void publish(LogRecord record) {
            if (record != null && record.getLevel().intValue() >= Level.SEVERE.intValue()) {
                messages.add(record.getMessage());
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}