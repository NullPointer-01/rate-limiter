package org.nullpointer.ratelimiter.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.nullpointer.ratelimiter.model.SubscriptionPlan;
import org.nullpointer.ratelimiter.model.config.hierarchical.HierarchicalRateLimitPolicy;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

public class PlanPolicyLoader {
    private final Map<SubscriptionPlan, HierarchicalRateLimitPolicy> planPolicies;

    private PlanPolicyLoader(String configPath) {
        this.planPolicies = loadFrom(configPath);
    }

    public static PlanPolicyLoader withConfig(String configPath) {
        if (configPath == null) throw new IllegalArgumentException("Config path cannot be null");
        return new PlanPolicyLoader(configPath);
    }

    public Map<SubscriptionPlan, HierarchicalRateLimitPolicy> getDefaultPolicies() {
        return Collections.unmodifiableMap(planPolicies);
    }

    private static Map<SubscriptionPlan, HierarchicalRateLimitPolicy> loadFrom(String configPath) {
        ObjectMapper yamlMapper = JsonMapper.builder(new YAMLFactory())
                .enable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .build();

        try (InputStream is = PlanPolicyLoader.class.getClassLoader().getResourceAsStream(configPath)) {
            if (is == null) {
                throw new IllegalStateException("Config file not found on classpath: " + configPath);
            }

            JsonNode root = yamlMapper.readTree(is);
            JsonNode plansNode = root.path("plans");
            if (plansNode.isMissingNode() || plansNode.isEmpty()) {
                throw new IllegalStateException("Config file contains no plans: " + configPath);
            }

            Map<String, HierarchicalRateLimitPolicy> plans = yamlMapper.convertValue(plansNode, new TypeReference<>() {});
            Map<SubscriptionPlan, HierarchicalRateLimitPolicy> result = new EnumMap<>(SubscriptionPlan.class);

            for (Map.Entry<String, HierarchicalRateLimitPolicy> entry : plans.entrySet()) {
                SubscriptionPlan plan = toPlan(entry.getKey());
                if (plan == null) throw new IllegalArgumentException("Invalid plan during default plan population");
                result.put(plan, entry.getValue());
            }

            return result;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load config file: " + configPath, e);
        }
    }

    private static SubscriptionPlan toPlan(String key) {
        try {
            return SubscriptionPlan.valueOf(key.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
