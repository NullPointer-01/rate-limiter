package org.nullpointer.ratelimiter.model.state;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = TokenBucketState.class, name = "token_bucket"),
    @JsonSubTypes.Type(value = FixedWindowCounterState.class, name = "fixed_window_counter"),
    @JsonSubTypes.Type(value = SlidingWindowState.class, name = "sliding_window"),
    @JsonSubTypes.Type(value = SlidingWindowCounterState.class, name = "sliding_window_counter")
})
public interface RateLimitState {
    /**
     * Returns a deep copy of this state object.
     */
    RateLimitState copy();
}
