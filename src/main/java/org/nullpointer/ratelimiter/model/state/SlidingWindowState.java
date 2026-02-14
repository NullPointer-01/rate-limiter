package org.nullpointer.ratelimiter.model.state;

import java.util.ArrayDeque;
import java.util.Deque;

public class SlidingWindowState implements RateLimitState {
    private long currentWindowCost;
    private final Deque<Request> deque;

    public SlidingWindowState() {
        this.deque = new ArrayDeque<>();
    }

    public void appendRequest(long cost, long timestamp) {
        deque.add(new Request(cost, timestamp));
        currentWindowCost += cost;
    }

    public long getCurrentWindowCost(long windowSizeMillis, long now) {
        while (!deque.isEmpty() && now - deque.peekFirst().timestamp > windowSizeMillis) {
            currentWindowCost -= deque.pollFirst().cost;
        }
        return currentWindowCost;
    }

    public boolean isWindowEmpty() {
        return deque.isEmpty();
    }

    public long getOldestTimestamp() {
        assert deque.peekFirst() != null;
        return deque.peekFirst().timestamp;
    }

    static class Request {
        long cost;
        long timestamp;

        Request(long cost, long timestamp) {
            this.cost = cost;
            this.timestamp = timestamp;
        }
    }
}
