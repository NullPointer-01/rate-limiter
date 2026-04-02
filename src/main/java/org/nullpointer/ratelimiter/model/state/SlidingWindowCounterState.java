package org.nullpointer.ratelimiter.model.state;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SlidingWindowCounterState implements RateLimitState {
    @JsonProperty("originNanos")
    private final long originNanos;

    @JsonProperty("windows")
    @JsonDeserialize(as = ConcurrentHashMap.class)
    private final Map<Long, Long> windows;

    @JsonIgnore
    private final int nWindows = 5; // N oldest windows

    @JsonCreator
    public SlidingWindowCounterState(@JsonProperty("originNanos") long originNanos) {
        this.windows = new ConcurrentHashMap<>();
        this.originNanos = originNanos;
    }

    public long getOriginNanos() {
        return originNanos;
    }

    public long getWindowCost(long windowId) {
        if (windows.containsKey(windowId)) {
            return windows.get(windowId);
        }

        return 0;
    }

    public void addCostToWindow(long cost, long windowId) {
        if (windows.containsKey(windowId)) {
            windows.put(windowId, windows.get(windowId) + cost);
            return;
        }

        clearOldestEntries();
        windows.put(windowId, cost);
    }

    /**
     * Delete the oldest window
     */
    private void clearOldestEntries() {
        if (windows.size() >= nWindows) {
            Long minWindowId = windows.keySet().stream().min(Comparator.comparingLong(key -> key)).get();
            windows.remove(minWindowId);
        }
    }

    @JsonProperty("windows")
    protected void setWindows(Map<Long, Long> windows) {
        this.windows.putAll(windows);
    }
}
