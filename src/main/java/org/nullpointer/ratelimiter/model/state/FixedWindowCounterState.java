package org.nullpointer.ratelimiter.model.state;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonMerge;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FixedWindowCounterState implements RateLimitState {
    @JsonProperty("windows")
    @JsonMerge
    @JsonDeserialize(as = ConcurrentHashMap.class)
    private final Map<Long, Long> windows;

    @JsonIgnore
    private final int nWindows = 5; // N oldest windows

    public FixedWindowCounterState() {
        this.windows = new ConcurrentHashMap<>();
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
     * Delete older windows
     */
    private void clearOldestEntries() {
        if (windows.size() >= nWindows) {
            Long minWindowId = windows.keySet().stream().min(Comparator.comparingLong(key -> key)).get();
            windows.remove(minWindowId);
        }
    }
}
