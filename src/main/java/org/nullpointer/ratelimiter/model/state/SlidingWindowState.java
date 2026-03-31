package org.nullpointer.ratelimiter.model.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public class SlidingWindowState implements RateLimitState {
    @JsonProperty("currentWindowCost")
    private long currentWindowCost;

    @JsonProperty("deque")
    private final Deque<Request> deque;

    public SlidingWindowState() {
        this.deque = new ArrayDeque<>();
    }

    @JsonCreator
    public SlidingWindowState(
            @JsonProperty("currentWindowCost") long currentWindowCost,
            @JsonProperty("deque") List<Request> deque) {
        this.currentWindowCost = currentWindowCost;
        this.deque = deque != null ? new ArrayDeque<>(deque) : new ArrayDeque<>();
    }

    public void appendRequest(long cost, long timestampNanos) {
        deque.add(new Request(cost, timestampNanos));
        currentWindowCost += cost;
    }

    /**
     * Allows only querying monotonically increasing nowNanos
     */
    public long getCurrentWindowCost(long windowSizeNanos, long nowNanos) {
        while (!deque.isEmpty() && nowNanos - deque.peekFirst().timestampNanos > windowSizeNanos) {
            currentWindowCost -= deque.pollFirst().cost;
        }
        return currentWindowCost;
    }

    public boolean isWindowEmpty() {
        return deque.isEmpty();
    }

    @JsonIgnore
    public long getOldestTimestampNanos() {
        assert deque.peekFirst() != null;
        return deque.peekFirst().timestampNanos;
    }

    /**
     * Returns the timestamp at which enough requests have expired to free up 'requiredCost'.
     */
    public long getTimestampWhenCapacityFreed(long windowSizeNanos, long nowNanos, long requiredCost) {
        long freedSoFar = 0;
        for (Request req : deque) {
            if (nowNanos - req.timestampNanos > windowSizeNanos) { // Skip already expired requests
                continue;
            }

            freedSoFar += req.cost;
            if (freedSoFar >= requiredCost) {
                return req.timestampNanos + windowSizeNanos;
            }
        }

        return nowNanos + windowSizeNanos; // After full window expires
    }

    static class Request {
        @JsonProperty("cost")
        long cost;

        @JsonProperty("timestampNanos")
        long timestampNanos;

        protected Request() {
        }

        Request(long cost, long timestampNanos) {
            this.cost = cost;
            this.timestampNanos = timestampNanos;
        }
    }
}
