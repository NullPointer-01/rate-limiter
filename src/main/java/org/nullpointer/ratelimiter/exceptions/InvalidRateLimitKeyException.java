package org.nullpointer.ratelimiter.exceptions;

public class InvalidRateLimitKeyException extends RuntimeException {
    public InvalidRateLimitKeyException(String message) {
        super(message);
    }
}
