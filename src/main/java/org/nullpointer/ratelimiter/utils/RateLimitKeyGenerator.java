package org.nullpointer.ratelimiter.utils;

import org.nullpointer.ratelimiter.exceptions.InvalidRateLimitKeyException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;

public class RateLimitKeyGenerator {

    public RateLimitKey generate(RateLimitScope scope, RequestContext context) {
        String identifier = extractIdentifier(scope, context);
        return new RateLimitKey(scope.getPrefix() + ":" + identifier);
    }

    // Should match with HierarchicalConfigurationManager.extractIdentifier()
    private String extractIdentifier(RateLimitScope scope, RequestContext context) {
        switch (scope) {
            case GLOBAL:
                return "global";
            case REGION:
                return context.getRegion()
                        .orElse("default-region");
            case TENANT:
                return context.getTenantId()
                        .orElseThrow(() -> new InvalidRateLimitKeyException("TenantId missing for tenant scope"));
            case SERVICE:
                return context.getServiceId()
                        .orElseThrow(() -> new InvalidRateLimitKeyException("ServiceId missing for service scope"));
            case USER:
                return context.getUserId()
                        .orElseThrow(() -> new InvalidRateLimitKeyException("UserId missing for user scope"));
            case IP:
                return context.getIpAddress()
                        .orElseThrow(() -> new InvalidRateLimitKeyException("IP missing for ip scope"));
            case ENDPOINT:
                return context.getHttpMethod().orElse("DEF") + ":" + context.getApiPath().orElse("/");
            default:
                throw new IllegalArgumentException("Unknown scope - " + scope);
        }
    }
}
