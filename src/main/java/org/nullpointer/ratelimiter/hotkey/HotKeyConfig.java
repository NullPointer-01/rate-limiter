package org.nullpointer.ratelimiter.hotkey;

public final class HotKeyConfig {

    private final boolean enabled;
    private final int hotThreshold;
    private final int warmThreshold;
    private final long detectionWindowMillis;
    private final long syncIntervalMillis;
    private final double hotLocalQuotaFraction;

    private HotKeyConfig(Builder builder) {
        this.enabled = builder.enabled;
        this.hotThreshold = builder.hotThreshold;
        this.warmThreshold = builder.warmThreshold;
        this.detectionWindowMillis = builder.detectionWindowMillis;
        this.syncIntervalMillis = builder.syncIntervalMillis;
        this.hotLocalQuotaFraction = builder.hotLocalQuotaFraction;
    }

    public static HotKeyConfig disabled() {
        return new Builder().enabled(false).build();
    }

    public static HotKeyConfig defaults() {
        return new Builder().build();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getHotThreshold() {
        return hotThreshold;
    }

    public int getWarmThreshold() {
        return warmThreshold;
    }

    /**
     * Duration of the sliding detection window used to count requests
     */
    public long getDetectionWindowMillis() {
        return detectionWindowMillis;
    }

    /**
     * How often the background synchroniser drains local counts back into the global state
     */
    public long getSyncIntervalMillis() {
        return syncIntervalMillis;
    }

    /**
     * Fraction of global capacity this instance may admit locally before syncing
     */
    public double getHotLocalQuotaFraction() {
        return hotLocalQuotaFraction;
    }

    public static final class Builder {
        private boolean enabled = true;
        private int hotThreshold = 500;
        private int warmThreshold = 100;
        private long detectionWindowMillis = 5_000;
        private long syncIntervalMillis = 2_000;
        private double hotLocalQuotaFraction = 1.0;

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder hotThreshold(int hotThreshold) {
            if (hotThreshold <= 0) throw new IllegalArgumentException("hotThreshold must be > 0");
            this.hotThreshold = hotThreshold;
            return this;
        }

        public Builder warmThreshold(int warmThreshold) {
            if (warmThreshold <= 0) throw new IllegalArgumentException("warmThreshold must be > 0");
            this.warmThreshold = warmThreshold;
            return this;
        }

        public Builder detectionWindowMillis(long detectionWindowMillis) {
            if (detectionWindowMillis <= 0) throw new IllegalArgumentException("detectionWindowMillis must be > 0");
            this.detectionWindowMillis = detectionWindowMillis;
            return this;
        }

        public Builder syncIntervalMillis(long syncIntervalMillis) {
            if (syncIntervalMillis <= 0) throw new IllegalArgumentException("syncIntervalMillis must be > 0");
            this.syncIntervalMillis = syncIntervalMillis;
            return this;
        }

        public Builder hotLocalQuotaFraction(double hotLocalQuotaFraction) {
            if (hotLocalQuotaFraction <= 0.0 || hotLocalQuotaFraction > 1.0) {
                throw new IllegalArgumentException("hotLocalQuotaFraction must be in (0, 1]");
            }
            this.hotLocalQuotaFraction = hotLocalQuotaFraction;
            return this;
        }

        public HotKeyConfig build() {
            if (warmThreshold >= hotThreshold) {
                throw new IllegalArgumentException(
                    "warmThreshold (" + warmThreshold + ") must be < hotThreshold (" + hotThreshold + ")");
            }
            return new HotKeyConfig(this);
        }
    }

    @Override
    public String toString() {
        return "HotKeyConfig{" +
                "enabled=" + enabled +
                ", hotThreshold=" + hotThreshold +
                ", warmThreshold=" + warmThreshold +
                ", detectionWindowMillis=" + detectionWindowMillis +
                ", syncIntervalMillis=" + syncIntervalMillis +
                ", hotLocalQuotaFraction=" + hotLocalQuotaFraction +
                '}';
    }
}
