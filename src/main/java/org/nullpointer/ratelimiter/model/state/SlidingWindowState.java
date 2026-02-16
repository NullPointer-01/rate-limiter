package org.nullpointer.ratelimiter.model.state;

import java.util.ArrayDeque;
import java.util.Deque;

public class SlidingWindowState implements RateLimitState {
    private long currentWindowCost;
    private final Deque<Request> deque;

    public SlidingWindowState() {
        this.deque = new ArrayDeque<>();
    }

    public void appendRequest(long cost, long timestampNanos) {
        deque.add(new Request(cost, timestampNanos));
        currentWindowCost += cost;
    }

    public long getCurrentWindowCost(long windowSizeNanos, long nowNanos) {
        while (!deque.isEmpty() && nowNanos - deque.peekFirst().timestampNanos > windowSizeNanos) {
            currentWindowCost -= deque.pollFirst().cost;
        }
        return currentWindowCost;
    }

    public boolean isWindowEmpty() {
        return deque.isEmpty();
    }

    public long getOldestTimestampNanos() {
        assert deque.peekFirst() != null;
        return deque.peekFirst().timestampNanos;
    }

    static class Request {
        long cost;
        long timestampNanos;

        Request(long cost, long timestampNanos) {
            this.cost = cost;
            this.timestampNanos = timestampNanos;
        }
    }
}
