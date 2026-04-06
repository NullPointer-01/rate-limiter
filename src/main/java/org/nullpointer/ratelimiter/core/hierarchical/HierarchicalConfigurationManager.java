package org.nullpointer.ratelimiter.core.hierarchical;

import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.utils.PlanPolicyLoader;
import org.nullpointer.ratelimiter.factory.StateRepositoryFactory;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RequestContext;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitLevel;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;
import org.nullpointer.ratelimiter.model.state.RateLimitState;
import org.nullpointer.ratelimiter.storage.config.ConfigRepository;
import org.nullpointer.ratelimiter.storage.state.AtomicStateRepository;
import org.nullpointer.ratelimiter.storage.state.StateRepository;

import java.util.Objects;

public class HierarchicalConfigurationManager extends ConfigurationManager {
    private final StateRepositoryFactory stateRepositoryFactory;

    public HierarchicalConfigurationManager(ConfigRepository configRepository, StateRepository stateRepository,
                                            PlanPolicyLoader planPolicyLoader,
                                            StateRepositoryFactory stateRepositoryFactory) {
        super(configRepository, stateRepository);

        Objects.requireNonNull(planPolicyLoader);
        Objects.requireNonNull(stateRepositoryFactory);

        // Populate default policies into repository, but do not overwrite existing policies.
        planPolicyLoader.getDefaultPolicies().forEach((plan, policy) -> {
            if (configRepository.getPlanPolicy(plan) == null) {
                configRepository.setPlanPolicy(plan, policy);
            }
        });
        this.stateRepositoryFactory = stateRepositoryFactory;
    }

    public void overridePlanPolicy(SubscriptionPlan plan, HierarchicalRateLimitPolicy policy) {
        this.configRepository.setPlanPolicy(plan, policy);
    }

    public void overrideScopedConfig(SubscriptionPlan plan, RateLimitScope scope,
                                       String identifier, RateLimitConfig config) {
        this.configRepository.setPlanScopedConfig(plan, scope, identifier, config);
    }

    public HierarchicalRateLimitPolicy resolvePolicy(RequestContext context) {
        SubscriptionPlan plan = context.getPlan() != null ? context.getPlan() : SubscriptionPlan.FREE;
        return this.configRepository.getPlanPolicy(plan);
    }

    public RateLimitConfig resolveConfig(RequestContext context, RateLimitLevel level) {
        SubscriptionPlan plan = context.getPlan();
        RateLimitScope scope = level.getScope();
        String identifier = extractIdentifier(scope, context);

        if (identifier != null) {
            return this.configRepository
                    .getPlanScopedConfig(plan, scope, identifier)
                    .orElse(level.getConfig());
        }
        return level.getConfig();
    }

    public StateRepository resolveStateRepository(RateLimitLevel level) {
        return stateRepositoryFactory.resolve(level.getStateRepositoryType());
    }

    public AtomicStateRepository resolveAtomicStateRepository(RateLimitLevel level) {
        return stateRepositoryFactory.resolveAtomic(level.getStateRepositoryType());
    }

    public RateLimitState getHierarchicalState(RateLimitKey key, StateRepository repo) {
        return repo.getHierarchicalState(key);
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
