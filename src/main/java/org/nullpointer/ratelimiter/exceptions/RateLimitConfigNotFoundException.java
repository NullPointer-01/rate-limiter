package org.nullpointer.ratelimiter.exceptions;

public class RateLimitConfigNotFoundException extends RuntimeException {
    public RateLimitConfigNotFoundException(String message) {
        super(message);
    }
}
