package org.nullpointer.ratelimiter.model;

public enum SubscriptionPlan {
    FREE,
    PREMIUM,
    ENTERPRISE;

    public String getPlanId() {
        return name().toLowerCase();
    }
}
