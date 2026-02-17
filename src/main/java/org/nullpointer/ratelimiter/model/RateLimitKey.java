package org.nullpointer.ratelimiter.model;

import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitKeyException;

public class RateLimitKey {
    private final String userId;
    private final String ipAddress;
    private final String domain;
    private final String api;

    private RateLimitKey(Builder builder) {
        this.userId = builder.userId;
        this.ipAddress = builder.ipAddress;
        this.api = builder.api;
        this.domain = builder.domain;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String toKey() {
        StringBuilder sb = new StringBuilder();
        appendPart(sb, "user", userId);
        appendPart(sb, "ip", ipAddress);
        appendPart(sb, "domain", domain);
        appendPart(sb, "api", api);

        String key = sb.toString();
        if (key.isBlank()) {
            throw new InvalidRateLimitKeyException("Rate limit key is invalid. Set atleast one value");
        }

        return key;
    }

    public static RateLimitKey fromKey(String key) {
        Builder builder = builder();
        if (key == null || key.isBlank()) {
            return builder.build();
        }

        String[] parts = key.split("\\|");
        for (String part : parts) {
            String[] keyValue = part.split("=", 2);
            if (keyValue.length != 2) {
                continue;
            }
            switch (keyValue[0]) {
                case "user" -> builder.setUserId(keyValue[1]);
                case "ip" -> builder.setIpAddress(keyValue[1]);
                case "domain" -> builder.setDomain(keyValue[1]);
                case "api" -> builder.setApi(keyValue[1]);
                default -> {}
            }
        }
        return builder.build();
    }

    private static void appendPart(StringBuilder sb, String key, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        if (!sb.isEmpty()) {
            sb.append(":");
        }
        sb.append(key).append("=").append(value);
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
            return new RateLimitKey(this);
        }
    }
}
