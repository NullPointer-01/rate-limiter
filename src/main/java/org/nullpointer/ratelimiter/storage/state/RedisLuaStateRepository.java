package org.nullpointer.ratelimiter.storage.state;

import org.nullpointer.ratelimiter.model.RateLimitKey;
import org.nullpointer.ratelimiter.model.RateLimitResult;
import org.nullpointer.ratelimiter.model.RequestTime;
import org.nullpointer.ratelimiter.model.config.FixedWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.RateLimitConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowConfig;
import org.nullpointer.ratelimiter.model.config.SlidingWindowCounterConfig;
import org.nullpointer.ratelimiter.model.config.TokenBucketConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisLuaStateRepository implements AtomicStateRepository {

    private static final String HSTATE_PREFIX = "rl:hstate:";

    // -----------------------------------------------------------------------
    // Token Bucket
    // State  : {"tokens": <float>, "lastMs": <int>}
    // ARGV   : nowMs, cost, capacity, refillTokens, refillIntervalMs
    // Returns: {allowed(0|1), remaining, resetAtMs, retryAfterMs}
    // -----------------------------------------------------------------------
    private static final String TOKEN_BUCKET_LUA = """
            local key = KEYS[1]
            local nowMs           = tonumber(ARGV[1])
            local cost            = tonumber(ARGV[2])
            local capacity        = tonumber(ARGV[3])
            local refillTokens    = tonumber(ARGV[4])
            local refillIntervalMs = tonumber(ARGV[5])

            local tokens, lastMs

            local raw = redis.call('GET', key)
            if raw == false then
                tokens = capacity
                lastMs = nowMs
            else
                local state = cjson.decode(raw)
                tokens = state['tokens']
                lastMs = state['lastMs']
            end

            if refillTokens > 0 and refillIntervalMs > 0 then
                local elapsedMs = nowMs - lastMs
                if elapsedMs > 0 then
                    local tokensToAdd = (refillTokens * elapsedMs) / refillIntervalMs
                    if tokensToAdd > 0 then
                        tokens = math.min(capacity, tokens + tokensToAdd)
                        local usedMs = (tokensToAdd * refillIntervalMs) / refillTokens
                        lastMs = math.min(nowMs, lastMs + usedMs)
                    end
                end
            end

            local allowed, remaining, resetAtMs, retryAfterMs

            if tokens >= cost then
                tokens = tokens - cost
                remaining = math.floor(tokens)
                allowed = 1
                retryAfterMs = 0
                local tokensToFill = capacity - tokens
                if tokensToFill <= 0 then
                    resetAtMs = nowMs
                else
                    resetAtMs = nowMs + math.ceil(tokensToFill * refillIntervalMs / refillTokens)
                end
            else
                remaining = math.floor(tokens)
                allowed = 0
                local tokensNeeded = cost - tokens
                retryAfterMs = math.ceil(tokensNeeded * refillIntervalMs / refillTokens)
                resetAtMs = nowMs + retryAfterMs
            end

            redis.call('SET', key, cjson.encode({tokens = tokens, lastMs = lastMs}))
            return {allowed, remaining, resetAtMs, retryAfterMs}
            """;

    // -----------------------------------------------------------------------
    // Fixed Window Counter
    // State  : {"windowId": <int>, "count": <int>}
    // ARGV   : nowMs, cost, capacity, windowSizeMs
    // Returns: {allowed(0|1), remaining, resetAtMs, retryAfterMs}
    // -----------------------------------------------------------------------
    private static final String FIXED_WINDOW_LUA = """
            local key          = KEYS[1]
            local nowMs        = tonumber(ARGV[1])
            local cost         = tonumber(ARGV[2])
            local capacity     = tonumber(ARGV[3])
            local windowSizeMs = tonumber(ARGV[4])

            local currentWindowId    = math.floor(nowMs / windowSizeMs)
            local nextWindowStartMs  = (currentWindowId + 1) * windowSizeMs
            local retryAfterMs       = math.max(0, nextWindowStartMs - nowMs)
            local resetAtMs          = nextWindowStartMs
            local count              = 0

            local raw = redis.call('GET', key)
            if raw ~= false then
                local state = cjson.decode(raw)
                if state['windowId'] == currentWindowId then
                    count = state['count']
                end
            end

            local allowed, remaining

            if count + cost <= capacity then
                count = count + cost
                remaining = math.max(0, capacity - count)
                allowed = 1
                retryAfterMs = 0
            else
                remaining = math.max(0, capacity - count)
                allowed = 0
            end

            redis.call('SET', key, cjson.encode({windowId = currentWindowId, count = count}))
            return {allowed, remaining, resetAtMs, retryAfterMs}
            """;

    // -----------------------------------------------------------------------
    // Sliding Window Counter  (weighted two-window approximation)
    // State  : {"originMs": <int>, "windows": {"<id>": <count>, ...}}
    // ARGV   : nowMs, cost, capacity, windowSizeMs
    // Returns: {allowed(0|1), remaining, resetAtMs, retryAfterMs}
    // -----------------------------------------------------------------------
    private static final String SLIDING_WINDOW_COUNTER_LUA = """
            local key          = KEYS[1]
            local nowMs        = tonumber(ARGV[1])
            local cost         = tonumber(ARGV[2])
            local capacity     = tonumber(ARGV[3])
            local windowSizeMs = tonumber(ARGV[4])

            local originMs, windows

            local raw = redis.call('GET', key)
            if raw == false then
                originMs = nowMs
                windows  = {}
            else
                local state = cjson.decode(raw)
                originMs = state['originMs']
                windows  = state['windows']
            end

            local elapsedSinceOrigin  = nowMs - originMs
            local currentWindowId     = math.floor(elapsedSinceOrigin / windowSizeMs)
            local currentWindowStartMs = originMs + (currentWindowId * windowSizeMs)
            local elapsed             = nowMs - currentWindowStartMs
            local windowProgress      = elapsed / windowSizeMs

            local currentKey      = tostring(currentWindowId)
            local prevKey         = tostring(currentWindowId - 1)
            local currentWindowUsed = windows[currentKey] or 0
            local prevWindowUsed    = windows[prevKey]    or 0

            local weightedCost = currentWindowUsed + prevWindowUsed * (1 - windowProgress)

            local allowed, remaining, resetAtMs, retryAfterMs

            if weightedCost + cost <= capacity then
                windows[currentKey] = currentWindowUsed + cost
                remaining    = math.max(0, math.floor(capacity - (weightedCost + cost)))
                allowed      = 1
                retryAfterMs = 0
                resetAtMs    = currentWindowStartMs + windowSizeMs
            else
                remaining    = math.max(0, math.floor(capacity - weightedCost))
                allowed      = 0
                retryAfterMs = math.max(0, math.floor((currentWindowStartMs + windowSizeMs) - nowMs))
                resetAtMs    = nowMs + retryAfterMs
            end

            -- Evict stale windows (keep current and previous only)
            local minWindowId = currentWindowId - 1
            for k in pairs(windows) do
                if tonumber(k) < minWindowId then
                    windows[k] = nil
                end
            end

            redis.call('SET', key, cjson.encode({originMs = originMs, windows = windows}))
            return {allowed, remaining, resetAtMs, retryAfterMs}
            """;

    // -----------------------------------------------------------------------
    // Sliding Window (request log)
    // State  : {"requests": [{"ms":<int>,"cost":<int>}, ...], "totalCost":<int>}
    // ARGV   : nowMs, cost, maxCost, windowSizeMs
    // Returns: {allowed(0|1), remaining, resetAtMs, retryAfterMs}
    // -----------------------------------------------------------------------
    private static final String SLIDING_WINDOW_LUA = """
            local key          = KEYS[1]
            local nowMs        = tonumber(ARGV[1])
            local cost         = tonumber(ARGV[2])
            local maxCost      = tonumber(ARGV[3])
            local windowSizeMs = tonumber(ARGV[4])

            local requests  = {}
            local totalCost = 0

            local raw = redis.call('GET', key)
            if raw ~= false then
                local state = cjson.decode(raw)
                requests    = state['requests']
                totalCost   = state['totalCost']
            end

            -- Evict requests that have left the sliding window
            local cutoffMs    = nowMs - windowSizeMs
            local newRequests = {}
            local newCost     = 0
            for _, req in ipairs(requests) do
                if req['ms'] > cutoffMs then
                    table.insert(newRequests, req)
                    newCost = newCost + req['cost']
                end
            end
            requests  = newRequests
            totalCost = newCost

            local allowed, remaining, resetAtMs, retryAfterMs

            if totalCost + cost <= maxCost then
                table.insert(requests, {ms = nowMs, cost = cost})
                totalCost    = totalCost + cost
                remaining    = math.max(0, maxCost - totalCost)
                allowed      = 1
                retryAfterMs = 0
                if #requests > 0 then
                    resetAtMs = requests[1]['ms'] + windowSizeMs
                else
                    resetAtMs = nowMs + windowSizeMs
                end
            else
                remaining = math.max(0, maxCost - totalCost)
                allowed   = 0
                -- Walk oldest requests first to find when enough capacity frees up
                local needed = (totalCost + cost) - maxCost
                local freed = 0
                retryAfterMs = windowSizeMs
                for _, req in ipairs(requests) do
                    freed = freed + req['cost']
                    if freed >= needed then
                        retryAfterMs = math.max(0, (req['ms'] + windowSizeMs) - nowMs)
                        break
                    end
                end
                resetAtMs = nowMs + retryAfterMs
            end

            redis.call('SET', key, cjson.encode({requests = requests, totalCost = totalCost}))
            return {allowed, remaining, resetAtMs, retryAfterMs}
            """;

    private final JedisPool jedisPool;
    private final Map<ScriptType, String> scriptShas;

    public RedisLuaStateRepository(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        this.scriptShas = loadScripts();
    }

    private Map<ScriptType, String> loadScripts() {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<ScriptType, String> shas = new HashMap<>();
            shas.put(ScriptType.TOKEN_BUCKET, jedis.scriptLoad(TOKEN_BUCKET_LUA));
            shas.put(ScriptType.FIXED_WINDOW, jedis.scriptLoad(FIXED_WINDOW_LUA));
            shas.put(ScriptType.SLIDING_WINDOW_COUNTER, jedis.scriptLoad(SLIDING_WINDOW_COUNTER_LUA));
            shas.put(ScriptType.SLIDING_WINDOW, jedis.scriptLoad(SLIDING_WINDOW_LUA));
            return shas;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public RateLimitResult atomicConsumeAndUpdate(RateLimitKey key, RateLimitConfig config,
                                                  RequestTime time, int cost) {
        String redisKey = HSTATE_PREFIX + key.toKey();
        long nowMs = time.currentTimeMillis();

        String sha;
        List<String> args;

        if (config instanceof TokenBucketConfig tb) {
            sha = scriptShas.get(ScriptType.TOKEN_BUCKET);
            args = List.of(
                    String.valueOf(nowMs),
                    String.valueOf(cost),
                    String.valueOf((long) tb.getCapacity()),
                    String.valueOf(tb.getRefillTokens()),
                    String.valueOf((long) tb.getRefillIntervalMillis())
            );
        } else if (config instanceof FixedWindowCounterConfig fw) {
            sha = scriptShas.get(ScriptType.FIXED_WINDOW);
            args = List.of(
                    String.valueOf(nowMs),
                    String.valueOf(cost),
                    String.valueOf(fw.getCapacity()),
                    String.valueOf(fw.getWindowSizeMillis())
            );
        } else if (config instanceof SlidingWindowCounterConfig swc) {
            sha = scriptShas.get(ScriptType.SLIDING_WINDOW_COUNTER);
            args = List.of(
                    String.valueOf(nowMs),
                    String.valueOf(cost),
                    String.valueOf(swc.getCapacity()),
                    String.valueOf(swc.getWindowSizeMillis())
            );
        } else if (config instanceof SlidingWindowConfig sw) {
            sha = scriptShas.get(ScriptType.SLIDING_WINDOW);
            args = List.of(
                    String.valueOf(nowMs),
                    String.valueOf(cost),
                    String.valueOf(sw.getMaxCost()),
                    String.valueOf(sw.getWindowSizeMillis())
            );
        } else {
            throw new IllegalArgumentException(
                    "Unsupported config type for Lua evaluation: " + config.getClass().getName());
        }

        try (Jedis jedis = jedisPool.getResource()) {
            List<Object> result = (List<Object>) jedis.evalsha(sha, List.of(redisKey), args);
            return parseResult(result, config);
        }
    }

    private RateLimitResult parseResult(List<Object> result, RateLimitConfig config) {
        long allowed = (Long) result.get(0);
        long remaining = (Long) result.get(1);
        long resetAtMs = (Long) result.get(2);
        long retryAfterMs = (Long) result.get(3);

        long limit = resolveLimit(config);

        return RateLimitResult.builder()
                .allowed(allowed == 1)
                .limit(limit)
                .remaining(remaining)
                .resetAtMillis(resetAtMs)
                .retryAfterMillis(retryAfterMs)
                .build();
    }

    private long resolveLimit(RateLimitConfig config) {
        if (config instanceof TokenBucketConfig tb) return (long) tb.getCapacity();
        if (config instanceof FixedWindowCounterConfig fw) return fw.getCapacity();
        if (config instanceof SlidingWindowCounterConfig swc) return swc.getCapacity();
        if (config instanceof SlidingWindowConfig sw) return sw.getMaxCost();
        return 0L;
    }

    private enum ScriptType {TOKEN_BUCKET, FIXED_WINDOW, SLIDING_WINDOW_COUNTER, SLIDING_WINDOW}
}
