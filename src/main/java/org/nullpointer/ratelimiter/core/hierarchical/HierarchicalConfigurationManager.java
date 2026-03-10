package org.nullpointer.ratelimiter.core.hierarchical;

import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.exceptions.RateLimitConfigNotFoundException;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;

public class HierarchicalConfigurationManager extends ConfigurationManager {
    private static final String DEFAULT_IDENTIFIER = "DEFAULT";

    public HierarchicalConfigurationManager(ConfigRepository configRepository, StateRepository stateRepository) {
        super(configRepository, stateRepository);
    }

    public void setHierarchyPolicy(HierarchicalRateLimitPolicy policy) {
        this.configRepository.setHierarchyPolicy(policy);
    }

    public HierarchicalRateLimitPolicy getHierarchyPolicy() {
        HierarchicalRateLimitPolicy hierarchyPolicy = this.configRepository.getHierarchyPolicy();
        if (hierarchyPolicy == null || hierarchyPolicy.isEmpty()) {
            throw new RateLimitConfigNotFoundException("No hierarchy policy configured");
        }
        return hierarchyPolicy;
    }

    public void setDefaultScopedConfig(RateLimitScope scope, RateLimitConfig config) {
        this.configRepository.setScopedConfig(scope, DEFAULT_IDENTIFIER, config);
    }

    public void setOverrideScopedConfig(RateLimitScope scope, String identifier, RateLimitConfig config) {
        if (DEFAULT_IDENTIFIER.equals(identifier)) {
            throw new IllegalArgumentException("Identifier cannot be DEFAULT");
        }
        this.configRepository.setScopedConfig(scope, identifier, config);
    }

    public RateLimitConfig resolveScopedConfig(RateLimitScope scope, RequestContext context) {
        // Check if any overrides are present
        String identifier = extractIdentifier(scope, context);
        if (identifier != null) {
            RateLimitConfig overrideConfig = this.configRepository.getScopedConfig(scope, identifier);
            if (overrideConfig != null) {
                return overrideConfig;
            }
        }

        // Fall back to default
        return this.configRepository.getScopedConfig(scope, DEFAULT_IDENTIFIER);
    }

    public void setHierarchicalState(RateLimitKey key, RateLimitState state) {
        this.stateRepository.setHierarchicalState(key, state);
    }

    public RateLimitState getHierarchicalState(RateLimitKey key) {
        return this.stateRepository.getHierarchicalState(key);
    }

    // Should match with RateLimitKeyGenerator.extractIdentifier()
    private String extractIdentifier(RateLimitScope scope, RequestContext context) {
        switch (scope) {
            case GLOBAL:
                return "global";
            case REGION:
                return context.getRegion().orElse(null);
            case TENANT:
                return context.getTenantId().orElse(null);
            case SERVICE:
                return context.getServiceId().orElse(null);
            case USER:
                return context.getUserId().orElse(null);
            case IP:
                return context.getIpAddress().orElse(null);
            case ENDPOINT:
                return (context.getHttpMethod().orElse("ANY") + ":" + context.getApiPath().orElse("/"));
            default:
                return null;
        }
    }
}
