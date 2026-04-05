package org.nullpointer.ratelimiter.storage.config;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryConfigRepository implements ConfigRepository {
	private final Map<String, RateLimitConfig> configMap;
	private final Map<String, HierarchicalRateLimitPolicy> planPolicyMap;
	private final Map<String, RateLimitConfig> planScopedConfigMap;

	private RateLimitConfig defaultConfig;

	public InMemoryConfigRepository() {
		this.configMap = new ConcurrentHashMap<>();
		this.planPolicyMap = new ConcurrentHashMap<>();
		this.planScopedConfigMap = new ConcurrentHashMap<>();
	}

	@Override
	public void setDefaultConfig(RateLimitConfig config) {
		this.defaultConfig = config;
	}

	@Override
	public RateLimitConfig getDefaultConfig() {
		return this.defaultConfig;
	}

	@Override
	public void setConfig(RateLimitKey key, RateLimitConfig config) {
		this.configMap.put(key.toKey(), config);
	}

	@Override
	public RateLimitConfig getConfig(RateLimitKey key) {
		return this.configMap.get(key.toKey());
	}

	@Override
	public RateLimitConfig getOrDefaultConfig(RateLimitKey key) {
		return this.configMap.getOrDefault(key.toKey(), defaultConfig);
	}

	@Override
	public void setPlanPolicy(SubscriptionPlan plan, HierarchicalRateLimitPolicy policy) {
		this.planPolicyMap.put(plan.getPlanId(), policy);
	}

	@Override
	public HierarchicalRateLimitPolicy getPlanPolicy(SubscriptionPlan plan) {
		return planPolicyMap.get(plan.getPlanId());
	}

	@Override
	public void setPlanScopedConfig(SubscriptionPlan plan, RateLimitScope scope, String identifier, RateLimitConfig config) {
		this.planScopedConfigMap.put(toPlanScopedKey(plan, scope, identifier), config);
	}

	@Override
	public Optional<RateLimitConfig> getPlanScopedConfig(SubscriptionPlan plan, RateLimitScope scope, String identifier) {
		return Optional.ofNullable(planScopedConfigMap.get(toPlanScopedKey(plan, scope, identifier)));
	}

	private String toPlanScopedKey(SubscriptionPlan plan, RateLimitScope scope, String identifier) {
		return plan.getPlanId() + ":" + scope.getPrefix() + ":" + identifier;
	}
}
