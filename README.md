# Distributed Rate Limiter

A production-grade, hierarchical distributed rate limiter in Java supporting multiple algorithms, multi-tenant policies, Redis-backed distributed state, hot key detection, circuit breaking, and extensible storage backends.

---

## Features

- **Multiple Rate Limiting Algorithms**
  - Token Bucket
  - Fixed Window Counter
  - Sliding Window
  - Sliding Window Counter

- **Hierarchical Rate Limiting**
  - Enforces independent limits at GLOBAL → REGION → TENANT → SERVICE → USER → IP → ENDPOINT scopes
  - Each scope can use a different algorithm and storage backend

- **Subscription Plan Policies**
  - Per-plan (FREE / PREMIUM / ENTERPRISE) rate limit configurations loaded from YAML
  - Entity-level overrides stored in config repository

- **Distributed Storage Backends**
  - `IN_MEMORY` – single-node, thread-safe ConcurrentHashMap
  - `IN_MEMORY_ATOMIC` – compare-and-swap atomic variant
  - `REDIS` – Redis-based distributed store
  - `ASYNC_REDIS` – write-behind async Redis sync
  - `REDIS_ATOMIC` – Lua-script-backed fully atomic Redis operations

- **Hot Key Detection & Mitigation**
  - Automatic promotion to hot-key mode with local quota reservation
  - Configurable thresholds and local quota fraction

- **Circuit Breaker**
  - Sliding-window circuit breaker (CLOSED → OPEN → HALF-OPEN)
  - Configurable fail-open / fail-closed fallback behavior

- **Observability**
  - Per-key metrics: allowed count, rejected count, rejection rate
  - Alerting service with configurable rejection-rate threshold
  - Structured logging throughout the pipeline

---

## Project Structure

```
src/
├── main/java/org/nullpointer/
│   ├── Main.java                        # Demo entry point
│   └── ratelimiter/
│       ├── algorithms/                  # Algorithm implementations
│       ├── client/                      # RateLimiter & HierarchicalRateLimiter SDKs
│       ├── core/                        # Decision engines & configuration managers
│       ├── factory/                     # Algorithm, CircuitBreaker, Redis, StateRepository factories
│       ├── hotkey/                      # Hot key detection & local rate limit engine
│       ├── instrumentation/             # Metrics
│       ├── model/                       # Domain models (key, result, config, state, context)
│       ├── resilience/                  # CircuitBreaker
│       ├── storage/                     # Config & state repositories
│       └── utils/                       # PlanPolicyLoader, key generation, time source
└── main/resources/
    ├── conf.props                       # Active config profile selector
    ├── rate-limiter-defaults.yml        # Default plan policies (Redis-backed)
    ├── rate-limiter-defaults-atomic.yml # Default plan policies (atomic backends)
    └── redis.yml                        # Redis connection settings
```

---

## Algorithms

| Algorithm | Best For | Burst Handling | Memory |
|---|---|---|---|
| Token Bucket | Smooth + burst traffic | ✅ Yes | Low |
| Fixed Window Counter | Simple per-window limits | ❌ Boundary spike | Minimal |
| Sliding Window Counter | Accurate rolling limits | ✅ Partial | Low |
| Sliding Window (log) | Exact rolling limits | ✅ Yes | Higher |

---

## Quick Start

### Single-Key Rate Limiter

```java
ConfigRepository configStore = new InMemoryConfigRepository();
StateRepository stateStore   = new InMemoryStateRepository();
ConfigurationManager mgr     = new ConfigurationManager(configStore, stateStore);

TokenBucketConfig config = new TokenBucketConfig(100, 10, 5, TimeUnit.SECONDS);
RateLimitKey key         = RateLimitKey.builder().setUserId("user123").build();
mgr.setConfig(key, config);

RateLimiter limiter  = new RateLimiter(mgr);
RateLimitResult result = limiter.process(key, 1);

System.out.println(result.isAllowed());      // true / false
System.out.println(result.getRemaining());   // remaining quota
System.out.println(result.getRetryAfterMillis()); // ms until retry
```

### Hierarchical Rate Limiter (Plan-based)

```java
StateRepositoryFactory factory = new StateRepositoryFactory();
factory.register(StateRepositoryType.IN_MEMORY, new InMemoryStateRepository());

HierarchicalConfigurationManager mgr = new HierarchicalConfigurationManager(
    new InMemoryConfigRepository(),
    new InMemoryStateRepository(),
    PlanPolicyLoader.withConfig("rate-limiter-defaults-atomic.yml"),
    factory
);

HierarchicalRateLimiter limiter = new HierarchicalRateLimiter(mgr);

RequestContext ctx = RequestContext.builder()
    .plan(SubscriptionPlan.FREE)
    .tenantId("acme")
    .userId("user42")
    .apiPath("/api/orders")
    .region("us-east-1")
    .build();

RateLimitResult result = limiter.process(ctx, 1);
```

---

## Architecture

```
Client Request
      │
      ▼
HierarchicalRateLimiter
      │
      ▼
HierarchicalRateLimitEngine
      │  resolves policy (plan defaults + entity overrides)
      │
      ├─► REGION  level  ──► StateRepository (Fixed Window  / IN_MEMORY_ATOMIC)
      ├─► TENANT  level  ──► StateRepository (Token Bucket  / REDIS_ATOMIC)
      ├─► USER    level  ──► StateRepository (Sliding Window / REDIS_ATOMIC)
      └─► ENDPOINT level ──► StateRepository (Token Bucket  / IN_MEMORY_ATOMIC)
                │
                │ first DENY short-circuits the chain
                ▼
          RateLimitResult
          { allowed, remaining, resetAtMs, retryAfterMs }
```

### Hot Key Path

```
Request → HotKeyLocalRateLimitEngine
               │
               ├─ classify(key) → COLD: delegate to normal engine
               │
               └─ HOT: consume from local quota
                         │
                         └─ quota exhausted → sync with Redis
```

### Circuit Breaker States

```
CLOSED ──(error rate > threshold)──► OPEN
  ▲                                     │
  └──(trial calls succeed)── HALF-OPEN ◄┘
                              (after wait period)
```

---

## Building & Testing

### Prerequisites

- Java 17+
- Maven 3.8+
- Redis (for distributed backend tests; jedis-mock used in unit tests)

### Build

```sh
mvn clean compile
```

### Run Tests

```sh
mvn test
```

### Run Demo

```sh
mvn exec:java -Dexec.mainClass="org.nullpointer.Main"
```

---

## Non-Functional Targets

| Metric | Target |
|---|---|
| P99 latency (in-memory) | < 1 ms |
| P99 latency (Redis) | < 5 ms |
| Throughput | 100K+ req/s per node |
| Availability | 99.99% |
| Consistency | Strong per key (atomic ops) |

---

## License

MIT
