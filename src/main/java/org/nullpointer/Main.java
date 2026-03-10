package org.nullpointer;

import org.nullpointer.ratelimiter.client.RateLimiter;
import org.nullpointer.ratelimiter.core.ConfigurationManager;
import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import org.nullpointer.ratelimiter.storage.config.ConfigStore;
import org.nullpointer.ratelimiter.storage.config.InMemoryConfigStore;
import org.nullpointer.ratelimiter.storage.state.InMemoryStateStore;
import org.nullpointer.ratelimiter.storage.state.StateStore;

import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        demoDefaultConfig();
        demoTokenBucketRateLimiter();
        demoteLimiter();
        demoFixedWindowCounterRateLimiter();
    }

    private static void demoFixedWindowCounterRateLimiter() {
        ConfigStore configStore = new InMemoryConfigStore();
        StateStore stateStore = new InMemoryStateStore();
        ConfigurationManager configManager = new ConfigurationManager(configStore, stateStore);

        FixedWindowCounterConfig config = new FixedWindowCounterConfig(10, 10, TimeUnit.SECONDS);
        RateLimiter rateLimiter = new RateLimiter(configManager);

        RateLimitKey key = RateLimitKey.builder().setUserId("user123").build();
        configManager.setConfig(key, config);

        RateLimitResult result = rateLimiter.process(key, 4);
        System.out.println(result);

        result = rateLimiter.process(key, 5);
        System.out.println(result);

        result = rateLimiter.process(key, 10);
        System.out.println(result);

        if (!result.isAllowed()) {
            try {
                Thread.sleep(result.getRetryAfterMillis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            result = rateLimiter.process(key, 10);
            System.out.println(result);
        }
    }

    private static void demoteLimiter() {
        ConfigStore configStore = new InMemoryConfigStore();
        StateStore stateStore = new InMemoryStateStore();
        ConfigurationManager configManager = new ConfigurationManager(configStore, stateStore);

        SlidingWindowConfig config = new SlidingWindowConfig(2, 5, TimeUnit.SECONDS);
        RateLimiter rateLimiter = new RateLimiter(configManager);

        RateLimitKey key = RateLimitKey.builder().setUserId("user123").build();
        configManager.setConfig(key, config);

        RateLimitResult result = rateLimiter.process(key);
        System.out.println(result);

        result = rateLimiter.process(key);
        System.out.println(result);

        result = rateLimiter.process(key);
        System.out.println(result);

        if (!result.isAllowed()) {
            try {
                Thread.sleep(result.getRetryAfterMillis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            result = rateLimiter.process(key, 1);
            System.out.println(result);
        }
    }

    private static void demoTokenBucketRateLimiter() {
        ConfigStore configStore = new InMemoryConfigStore();
        StateStore stateStore = new InMemoryStateStore();
        ConfigurationManager configManager = new ConfigurationManager(configStore, stateStore);

        TokenBucketConfig config = new TokenBucketConfig(100, 10, 5, TimeUnit.SECONDS);
        RateLimiter rateLimiter = new RateLimiter(configManager);

        RateLimitKey key = RateLimitKey.builder().setUserId("user123").build();
        configManager.setConfig(key, config);

        RateLimitResult result = rateLimiter.process(key, 1);
        System.out.println(result);
    }

    private static void demoDefaultConfig() {
        ConfigStore configStore = new InMemoryConfigStore();
        StateStore stateStore = new InMemoryStateStore();
        ConfigurationManager configManager = new ConfigurationManager(configStore, stateStore);

        TokenBucketConfig defaultConfig = new TokenBucketConfig(100, 10, 5, TimeUnit.SECONDS);
        configManager.setDefaultConfig(defaultConfig);

        RateLimiter rateLimiter = new RateLimiter(configManager);

        RateLimitKey key = RateLimitKey.builder().setUserId("user123").build();

        RateLimitResult result = rateLimiter.process(key, 1);
        System.out.println(result);

        result = rateLimiter.process(key, 100);
        System.out.println(result);

        if (!result.isAllowed()) {
            try {
                Thread.sleep(result.getRetryAfterMillis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            result = rateLimiter.process(key, 100);
            System.out.println(result);
        }
    }
}