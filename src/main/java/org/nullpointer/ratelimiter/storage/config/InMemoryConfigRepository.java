package org.nullpointer.ratelimiter.storage.config;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;
import org.nullpointer.ratelimiter.model.config.hierarchical.RateLimitScope;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryConfigRepository implements ConfigRepository {
	private final Map<String, RateLimitConfig> configMap;
	private final Map<String, RateLimitConfig> scopedConfigMap;

	private RateLimitConfig defaultConfig;
	private HierarchicalRateLimitPolicy hierarchyPolicy;

	public InMemoryConfigRepository() {
		this.configMap = new ConcurrentHashMap<>();
		this.scopedConfigMap = new ConcurrentHashMap<>();
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
	public void setHierarchyPolicy(HierarchicalRateLimitPolicy policy) {
		this.hierarchyPolicy = policy;
	}

	@Override
	public HierarchicalRateLimitPolicy getHierarchyPolicy() {
		return this.hierarchyPolicy;
	}

	@Override
	public void setScopedConfig(RateLimitScope scope, String identifier, RateLimitConfig config) {
		this.scopedConfigMap.put(toScopedConfigKey(scope, identifier), config);
	}

	@Override
	public RateLimitConfig getScopedConfig(RateLimitScope scope, String identifier) {
		return this.scopedConfigMap.get(toScopedConfigKey(scope, identifier));
	}

	private String toScopedConfigKey(RateLimitScope scope, String identifier) {
		return scope.getPrefix() + ":" + identifier;
	}
}
