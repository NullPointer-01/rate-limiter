package org.nullpointer.ratelimiter.alerting;

import org.nullpointer.ratelimiter.instrumentation.RateLimiterMetrics;
import org.nullpointer.ratelimiter.model.alert.Alert;
import org.nullpointer.ratelimiter.model.alert.ThresholdRejectionReachedAlert;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.nullpointer.ratelimiter.model.alert.AlertConstants.MONITOR_INTERVAL_IN_SECONDS;
import static org.nullpointer.ratelimiter.model.alert.AlertConstants.THRESHOLD_REJECTION_RATE;

public class AlertingService {
    private static final Logger logger = Logger.getLogger(AlertingService.class.getName());

    private final RateLimiterMetrics metrics;
    private final ScheduledExecutorService executor;

    public AlertingService(RateLimiterMetrics metrics) {
        this.metrics = metrics;
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "Thread - Alert Service");
            t.setDaemon(true);
            return t;
        };

        this.executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    public void start() {
        this.executor.scheduleAtFixedRate(
                this::monitor,
                0,
                MONITOR_INTERVAL_IN_SECONDS,
                TimeUnit.SECONDS
        );
    }

    public void monitor() {
        monitorRejectionRate();
    }

    private void monitorRejectionRate() {
        double rejectionRate = metrics.getRejectionRate();

        if (rejectionRate >= THRESHOLD_REJECTION_RATE) {
            Alert alert = new ThresholdRejectionReachedAlert(rejectionRate);
            sendAlert(alert);
        }
    }

    private void sendAlert(Alert alert) {
        logger.log(Level.SEVERE, alert.asMessage());
    }
}
