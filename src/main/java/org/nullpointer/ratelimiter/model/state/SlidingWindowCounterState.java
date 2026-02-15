package org.nullpointer.ratelimiter.model.state;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SlidingWindowCounterState implements RateLimitState {
    private final Map<Long, Long> windows;
    private final int nWindows = 5; // N oldest windows

    public SlidingWindowCounterState() {
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
            windows.clear();
        }
    }
}
