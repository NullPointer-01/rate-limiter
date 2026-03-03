package org.nullpointer.ratelimiter.model;

import java.util.Optional;

public class RequestContext {
    private final String tenantId;
    private final String userId;
    private final String ipAddress;
    private final String serviceId;
    private final String apiPath;
    private final String httpMethod;
    private final String region;

    private RequestContext(Builder builder) {
        this.tenantId = builder.tenantId;
        this.userId = builder.userId;
        this.ipAddress = builder.ipAddress;
        this.serviceId = builder.serviceId;
        this.apiPath = builder.apiPath;
        this.httpMethod = builder.httpMethod;
        this.region = builder.region;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<String> getTenantId() {
        return Optional.ofNullable(tenantId);
    }

    public Optional<String> getUserId() {
        return Optional.ofNullable(userId);
    }

    public Optional<String> getIpAddress() {
        return Optional.ofNullable(ipAddress);
    }

    public Optional<String> getServiceId() {
        return Optional.ofNullable(serviceId);
    }

    public Optional<String> getApiPath() {
        return Optional.ofNullable(apiPath);
    }

    public Optional<String> getHttpMethod() {
        return Optional.ofNullable(httpMethod);
    }

    public Optional<String> getRegion() {
        return Optional.ofNullable(region);
    }

    public static class Builder {
        private String tenantId;
        private String userId;
        private String ipAddress;
        private String serviceId;
        private String apiPath;
        private String httpMethod;
        private String region;

        public Builder tenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder userId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder ipAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public Builder serviceId(String serviceId) {
            this.serviceId = serviceId;
            return this;
        }

        public Builder apiPath(String apiPath) {
            this.apiPath = apiPath;
            return this;
        }

        public Builder httpMethod(String httpMethod) {
            this.httpMethod = httpMethod;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public RequestContext build() {
            return new RequestContext(this);
        }
    }
}
