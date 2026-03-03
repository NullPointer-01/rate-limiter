package org.nullpointer.ratelimiter.model.config.hierarchical;

public enum RateLimitScope {
    GLOBAL("global", 0),
    REGION("region", 10),
    TENANT("tenant", 20),
    SERVICE("service", 30),
    USER("user", 40),
    IP("ip", 50),
    ENDPOINT("api", 60);

    private final String prefix;
    private final int order;

    RateLimitScope(String prefix, int order) {
        this.prefix = prefix;
        this.order = order;
    }

    public String getPrefix() {
        return prefix;
    }

    public int getOrder() {
        return order;
    }
}
