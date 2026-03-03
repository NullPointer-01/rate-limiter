package org.nullpointer.ratelimiter.model.alert;

import org.nullpointer.ratelimiter.alerting.AlertType;

public class ThresholdRejectionReachedAlert implements Alert {
    private final double rejectionRate;

    public ThresholdRejectionReachedAlert(double rejectionRate) {
        this.rejectionRate = rejectionRate;
    }

    @Override
    public AlertType getAlertType() {
        return AlertType.THRESHOLD_REJECTION_REACHED;
    }

    @Override
    public String asMessage() {
        return "ThresholdRejectionReachedAlert{" +
                "rejectionRate=" + rejectionRate +
                '}';
    }
}
