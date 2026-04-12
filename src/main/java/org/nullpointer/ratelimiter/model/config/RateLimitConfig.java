package org.nullpointer.ratelimiter.model.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;
import org.nullpointer.ratelimiter.model.state.RateLimitState;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = TokenBucketConfig.class, name = "token_bucket"),
    @JsonSubTypes.Type(value = FixedWindowCounterConfig.class, name = "fixed_window_counter"),
    @JsonSubTypes.Type(value = SlidingWindowConfig.class, name = "sliding_window"),
    @JsonSubTypes.Type(value = SlidingWindowCounterConfig.class, name = "sliding_window_counter")
})
public interface RateLimitConfig {
   RateLimitingAlgorithm getAlgorithm();

   RateLimitState initialRateLimitState(long nanoTime);

   /**
    * Returns the maximum number of requests (or tokens/cost units) allowed per window.
    */
   long getCapacity();

   /**
    * Returns the rate-limit window duration in milliseconds.
    * For fixed/sliding window algorithms this is the explicit window size.
    * For token bucket this is the time required to refill the bucket from 0 to capacity
    */
   long getWindowSizeMillis();
}
