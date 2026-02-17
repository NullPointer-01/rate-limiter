package org.nullpointer.ratelimiter.model;

import org.junit.jupiter.api.Test;
import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitKeyException;

import static org.junit.jupiter.api.Assertions.*;

class RateLimitKeyTest {

    @Test
    void buildsCompositeKeyAndParsesBack() {
        RateLimitKey key = RateLimitKey.builder()
                .setUserId("u1")
                .setIpAddress("10.0.0.1")
                .setDomain("example.com")
                .setApi("GET:/v1/test")
                .build();

        String serialized = key.toKey();
        RateLimitKey parsed = RateLimitKey.fromKey(serialized);

        assertEquals(serialized, parsed.toKey());
    }

    @Test
    void throwsOnEmptyKey() {
        RateLimitKey.Builder builder = RateLimitKey.builder();
        RateLimitKey key = builder.build();
        assertThrows(InvalidRateLimitKeyException.class, key::toKey);
    }
}
