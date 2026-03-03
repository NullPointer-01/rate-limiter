package org.nullpointer.ratelimiter.model;

import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitKeyException;

import java.util.Objects;

public class RateLimitKey {
    private final String key;

    public RateLimitKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        this.key = key;
    }

    public String toKey() {
        if (key.isBlank()) {
            throw new InvalidRateLimitKeyException("Rate limit key is invalid. Set atleast one value");
        }
        return key;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static RateLimitKey fromKey(String key) {
        if (key == null || key.isBlank()) {
            return builder().build();
        }
        return new RateLimitKey(key);
    }

    @Override
    public String toString() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RateLimitKey rateLimitKey = (RateLimitKey) o;
        return Objects.equals(key, rateLimitKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    public static class Builder {
        private String userId;
        private String ipAddress;
        private String domain;
        private String api;

        public Builder setUserId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder setIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder setDomain(String domain) {
            this.domain = domain;
            return this;
        }

        public Builder setApi(String api) {
            this.api = api;
            return this;
        }

        public RateLimitKey build() {
            StringBuilder sb = new StringBuilder();
            appendPart(sb, "user", userId);
            appendPart(sb, "ip", ipAddress);
            appendPart(sb, "domain", domain);
            appendPart(sb, "api", api);

            return new RateLimitKey(sb.toString());
        }

        private static void appendPart(StringBuilder sb, String key, String value) {
            if (value == null || value.isBlank()) {
                return;
            }
            if (!sb.isEmpty()) { // No colon prefix for the first part
                sb.append(":");
            }
            sb.append(key).append("=").append(value);
        }
    }
}
