package org.nullpointer.ratelimiter.model.alert;

import org.nullpointer.ratelimiter.alerting.AlertType;

public interface Alert {
    AlertType getAlertType();

    String asMessage();
}
