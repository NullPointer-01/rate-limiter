package org.nullpointer.ratelimiter.exceptions;

public class InvalidRateLimitCostException extends RuntimeException {
    public InvalidRateLimitCostException(String message) {
        super(message);
    }
}
