package org.nullpointer.ratelimiter.circuitbreaker;

import java.util.concurrent.atomic.AtomicReferenceArray;

public class BucketedSlidingWindow {
    private final AtomicReferenceArray<TimeBucket> buckets;
    private final int numBuckets;
    private final long bucketSizeNanos;
    private final Clock clock;

    public BucketedSlidingWindow(int numBuckets, long bucketSizeNanos, Clock clock) {
        this.numBuckets = numBuckets;
        this.bucketSizeNanos = bucketSizeNanos;
        this.clock = clock;
        this.buckets = new AtomicReferenceArray<>(numBuckets);
    }

    private TimeBucket getCurrentBucket() {
        long now = clock.nanoTime();
        long currentWindowStart = (now / bucketSizeNanos) * bucketSizeNanos;
        int index = (int) ((now / bucketSizeNanos) % numBuckets);

        while (true) {
            TimeBucket existing = buckets.get(index);

            if (existing == null) {
                // CAS a new bucket
                TimeBucket newBucket = new TimeBucket(currentWindowStart);
                if (buckets.compareAndSet(index, null, newBucket)) {
                    return newBucket;
                }
                // Loop back and re-read the bucket, as another thread might have completed the CAS
                continue;
            }

            // Current bucket is valid for the current time window
            if (existing.getWindowStartNanos() == currentWindowStart) {
                return existing;
            }

            // Stale bucket — belongs to a previous cycle, replace it
            TimeBucket newBucket = new TimeBucket(currentWindowStart);
            if (buckets.compareAndSet(index, existing, newBucket)) {
                return newBucket;
            }
        }
    }

    public void recordSuccess() {
        getCurrentBucket().incrementSuccess();
    }

    public void recordError() {
        getCurrentBucket().incrementError();
    }

    /**
     * Computes the failure rate across all non-stale buckets.
     */
    public double getFailureRate() {
        long now = clock.nanoTime();
        long windowDurationNanos = (long) numBuckets * bucketSizeNanos;
        long windowStart = now - windowDurationNanos;

        long totalSuccess = 0;
        long totalError = 0;

        for (int i = 0; i < numBuckets; i++) {
            TimeBucket bucket = buckets.get(i);
            if (bucket != null && bucket.getWindowStartNanos() > windowStart) {
                totalSuccess += bucket.sumSuccess();
                totalError += bucket.sumError();
            }
        }

        long total = totalSuccess + totalError;
        if (total == 0) return 0.0;

        return 100.0 * totalError / total;
    }

    /**
     * Returns the total number of calls (success + error) across all non-stale buckets.
     */
    public long getTotalCount() {
        long now = clock.nanoTime();
        long windowDurationNanos = (long) numBuckets * bucketSizeNanos;
        long windowStart = now - windowDurationNanos;

        long total = 0;

        for (int i = 0; i < numBuckets; i++) {
            TimeBucket bucket = buckets.get(i);
            if (bucket != null && bucket.getWindowStartNanos() > windowStart) {
                total += bucket.sumSuccess() + bucket.sumError();
            }
        }

        return total;
    }

    public void reset() {
        for (int i = 0; i < numBuckets; i++) {
            buckets.set(i, null);
        }
    }
}
