package org.nullpointer.ratelimiter.algorithms;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TokenBucketAlgorithmTest {

    @Test
    void allowsWithinCapacityAndThenDenies() {
        TokenBucketConfig config = new TokenBucketConfig(5, 1, 1, TimeUnit.SECONDS);
        RateLimitState state = config.initialRateLimitState();
        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult r1 = algorithm.tryConsume(key, config, state, 3);
        RateLimitResult r2 = algorithm.tryConsume(key, config, state, 3);

        assertTrue(r1.isAllowed());
        assertEquals(2, r1.getRemaining());
        assertFalse(r2.isAllowed());
        assertTrue(r2.getRetryAfterMillis() >= 0);
    }

    @Test
    void refillsOverTimeAllowsAgain() throws InterruptedException {
        TokenBucketConfig config = new TokenBucketConfig(1, 1, 1, TimeUnit.MILLISECONDS);
        RateLimitState state = config.initialRateLimitState();
        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult first = algorithm.tryConsume(key, config, state, 1);
        RateLimitResult second = algorithm.tryConsume(key, config, state, 1);
        long waitMillis = Math.max(1L, second.getRetryAfterMillis() + 1);
        Thread.sleep(waitMillis);
        RateLimitResult third = algorithm.tryConsume(key, config, state, 1);

        assertTrue(first.isAllowed());
        assertFalse(second.isAllowed());
        assertTrue(third.isAllowed());
    }

    @Test
    void costGreaterThanCapacityDenies() {
        TokenBucketConfig config = new TokenBucketConfig(2, 1, 1, TimeUnit.SECONDS);
        RateLimitState state = config.initialRateLimitState();
        TokenBucketAlgorithm algorithm = new TokenBucketAlgorithm();
        RateLimitKey key = RateLimitKey.builder().setUserId("user-1").build();

        RateLimitResult result = algorithm.tryConsume(key, config, state, 5);

        assertFalse(result.isAllowed());
        assertEquals(2, result.getRemaining());
    }
}
