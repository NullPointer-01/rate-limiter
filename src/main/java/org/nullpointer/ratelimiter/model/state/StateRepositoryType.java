package org.nullpointer.ratelimiter.model.state;

public enum StateRepositoryType {
    IN_MEMORY,
    REDIS,
    ASYNC_REDIS,
    IN_MEMORY_ATOMIC,
    REDIS_ATOMIC
}
