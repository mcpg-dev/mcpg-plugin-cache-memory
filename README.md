# In-Memory Cache — `dev.mcpg.cache.memory`

> class `cache` · `native` · package `mcpg-plugin-cache-memory` · artifact `libmcpg_plugin_cache_memory.so` · Apache-2.0

A `cache` plugin that keeps cached state in the gateway process itself, backed by
a bounded `moka` cache. Entries are LRU-evicted once the configured capacity is
reached, and each entry carries its own TTL rather than sharing a global one.
There is no network, no connection pool, and no external service — every
operation is local CPU, so nothing about it can fail transiently. Reach for it on
a single-instance deployment, or as a dependency-free stand-in for the Redis
cache in development and tests; a multi-replica deployment that needs one cache
shared across replicas wants `dev.mcpg.cache.redis` instead, because this cache
is per-process by construction.

## What it does
- Serves **any** namespace (`serves_any_namespace()` is `true`), so a single
  configured instance can back every namespace bound to it, and keys are scoped
  by the `(namespace, key)` pair so two namespaces never collide.
- Applies a per-entry TTL through a `moka` expiry policy driven by the stored
  value's own duration — each entry expires on its own clock.
- Implements atomic per-key counters: `incr` runs under `moka`'s key-level
  serialization, so concurrent increments on the same key accumulate correctly.
  Counters are stored as decimal-ASCII bytes and read back as ordinary values.
- Invalidates one namespace at a time; a namespace clear walks a predicate and
  forces the pending purge so a subsequent read observes the eviction, and it
  never touches other namespaces.
- Refuses to register on unparseable or invalid config — a misconfigured cache
  fails the gateway's boot instead of silently degrading to defaults.
- Declares no required capabilities: it opens no sockets and touches no files,
  so its `plugins[]` entry needs no `granted_capabilities`.

## Configuration
Loaded from the flat top-level `plugins:` list with `class: cache`. The block
under the entry's `config:` key is handed to the plugin verbatim.

```yaml
plugins:
  - id: dev.mcpg.cache.memory
    class: cache
    kind: native
    source:
      path: ./plugins/libmcpg_plugin_cache_memory.so
      # or, platform-agnostic:
      # oci: ghcr.io/mcpg-dev/source-code/plugins/cache-memory:protocol-1
    config:
      max_capacity: 50000
```

| Field | Type | Default | Description |
|---|---|---|---|
| `max_capacity` | u64 | `10000` | Maximum entry count before LRU eviction. Must be greater than zero. |

Unknown fields are rejected.

The gateway also registers a built-in in-memory cache of its own
(`dev.mcpg.builtin.cache.memory`) that is always present. This crate is the
loadable sibling: reach for it when you want the in-memory cache to be a signed,
independently versioned artifact you can swap for the Redis one without changing
anything else about the deployment.

## Operations

| Operation | Behaviour |
|---|---|
| `get(ns, key)` | The stored bytes on a hit, nothing on a miss or after expiry. |
| `put(ns, key, value, ttl_ms)` | Insert or overwrite with a per-entry TTL. Never fails. |
| `delete(ns, key)` | Remove one key. Deleting an absent key is a no-op. |
| `clear(ns)` | Remove every key in one namespace, leaving others untouched. |
| `incr(ns, key, by, ttl_ms)` | Atomically add `by` (which may be negative) and return the new value; an absent key starts at zero. |

A TTL of zero milliseconds means "expire on the next tick", matching the cache
contract across every backend, so it is written as a one-millisecond expiry
rather than as "no expiry" — there is deliberately no infinite-TTL form.

## Observability
The gateway wraps every namespace binding in a metering decorator, so this
plugin's operations show up as `mcpg_cache_ops_total`
(labels `plugin_id`, `namespace`, `op`, `outcome`),
`mcpg_cache_op_latency_seconds` (labels `plugin_id`, `namespace`, `op`), and
`mcpg_cache_errors_total` (labels `plugin_id`, `namespace`, `kind`) without the
plugin emitting anything itself. Cache keys never appear in a label — the key
space is unbounded and would blow up cardinality.

## Build
Default feature set is OFF (avoids `mcpg_plugin_register` linker
collisions in the workspace build); opt in to the cdylib:

```bash
cargo build -p mcpg-plugin-cache-memory --features cdylib-export --release   # → target/release/libmcpg_plugin_cache_memory.so
```

## Testing
The suite is fully offline — no service, no container, no network:

```bash
cargo test -p mcpg-plugin-cache-memory
```

It covers per-entry TTL expiry, namespace-scoped clear, counter accumulation
including negative deltas, `incr` atomicity under eight concurrent threads, and
the fail-closed config rejections.

## Sign & load (production)
Sign the artifact, pin/verify via the entry's `signature:` block, and honour
revocations. See <https://mcpg.dev/docs/security/plugin-security>.

## See also
- Plugin classes and the plugin ABI: <https://mcpg.dev/docs/plugins/plugins-and-protocol>
- Full gateway config schema: <https://mcpg.dev/docs/reference/configuration>
- The cross-replica alternative: `libs/plugins/cache/redis`
