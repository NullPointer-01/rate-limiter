package org.nullpointer.ratelimiter.algorithms.factory;

import org.nullpointer.ratelimiter.algorithms.AlgorithmType;
import org.nullpointer.ratelimiter.algorithms.RateLimitingAlgorithm;

import java.util.HashMap;
import java.util.Map;

public class AlgorithmFactory {
    private static final Map<AlgorithmType, RateLimitingAlgorithm> algorithms;

    static {
        algorithms = new HashMap<>();
        for (AlgorithmType type : AlgorithmType.values()) {
            algorithms.put(type, type.getAlgorithm());
        }
    }

    private AlgorithmFactory() {}

    public static RateLimitingAlgorithm getAlgorithmByType(AlgorithmType type) {
        return algorithms.get(type);
    }
}
