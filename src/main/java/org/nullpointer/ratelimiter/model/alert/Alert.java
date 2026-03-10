package org.nullpointer.ratelimiter.model.alert;

public interface Alert {
    AlertType getAlertType();

    String asMessage();
}
