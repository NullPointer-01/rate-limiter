package org.nullpointer.ratelimiter.utils;

import org.nullpointer.ratelimiter.model.RequestTime;

public interface TimeSource {
    // Returns current time in millis
    long currentTimeMillis();

    // Used for measuring elapsed time
    long nanoTime();

    default RequestTime capture() {
        return new RequestTime(currentTimeMillis(), nanoTime());
    }
}
