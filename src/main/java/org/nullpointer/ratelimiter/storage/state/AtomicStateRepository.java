package org.nullpointer.ratelimiter.storage.state;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;

public interface AtomicStateRepository {
    RateLimitResult atomicConsumeAndUpdate(RateLimitKey key, RateLimitConfig config, RequestTime time, int cost);
}
