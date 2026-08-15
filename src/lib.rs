//! `dev.mcpg.cache.memory` — in-process `cache` plugin.
//!
//! A bounded (LRU-evicted) in-memory cache with **per-entry TTL**, backed by
//! `moka::sync::Cache`. No network, no backend — every operation is local CPU.
//! Useful as a single-instance response/counter cache or a dev/test stand-in
//! for the Redis cache. Counters (`incr`) are atomic per key via moka's
//! key-level serialization. Fails closed on bad config.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mcpg_plugin_protocol::cache::CacheError;
use mcpg_plugin_protocol::{PluginClass, PluginManifest};
use mcpg_plugin_sdk::ffi::SyncCachePlugin;
use moka::Expiry;
use moka::sync::Cache;
use serde::Deserialize;
use thiserror::Error;

const PLUGIN_ID: &str = "dev.mcpg.cache.memory";

type CacheKey = (String, String);

/// A cached value carrying its own TTL so each entry expires independently
/// (moka's builder-level `time_to_live` is global; the cache trait gives a
/// distinct `ttl_ms` per write).
#[derive(Debug, Clone)]
struct Entry {
    bytes: Vec<u8>,
    ttl: Duration,
}

/// Drives moka's per-entry expiry off the stored `Entry::ttl`.
struct TtlExpiry;

impl Expiry<CacheKey, Entry> for TtlExpiry {
    fn expire_after_create(&self, _k: &CacheKey, v: &Entry, _now: Instant) -> Option<Duration> {
        Some(v.ttl)
    }
    fn expire_after_update(
        &self,
        _k: &CacheKey,
        v: &Entry,
        _now: Instant,
        _cur: Option<Duration>,
    ) -> Option<Duration> {
        Some(v.ttl)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryCacheConfig {
    /// Maximum number of entries before LRU eviction. Must be > 0.
    #[serde(default = "default_max_capacity")]
    pub max_capacity: u64,
}

fn default_max_capacity() -> u64 {
    10_000
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid cache.memory config JSON: {0}")]
    ParseError(#[from] serde_json::Error),
    #[error("cache.memory: max_capacity must be > 0")]
    Invalid,
}

impl MemoryCacheConfig {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = serde_json::from_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.max_capacity == 0 {
            return Err(ConfigError::Invalid);
        }
        Ok(())
    }
}

/// Translate a trait `ttl_ms` to a moka `Duration`. Per the cache contract,
/// `ttl_ms == 0` means "expire on the next tick" (mirrors the Redis `px=1`).
fn ttl_to_duration(ttl_ms: u64) -> Duration {
    if ttl_ms == 0 {
        Duration::from_millis(1)
    } else {
        Duration::from_millis(ttl_ms)
    }
}

pub struct MemoryCache {
    inner: Arc<Inner>,
}

struct Inner {
    manifest: PluginManifest,
    cache: Cache<CacheKey, Entry>,
}

impl MemoryCache {
    /// SDK factory. Fails closed: a bad config panics (→ null handle → boot
    /// Err), the uniform plugin convention.
    pub fn from_config_json(config_json: &str) -> Self {
        let config = MemoryCacheConfig::parse(config_json).unwrap_or_else(|err| {
            tracing::error!(
                plugin_id = PLUGIN_ID,
                error = %err,
                "cache.memory: config parse failed; refusing to register"
            );
            panic!("cache.memory config parse failed: {err}")
        });

        let cache = Cache::builder()
            .max_capacity(config.max_capacity)
            .expire_after(TtlExpiry)
            // Required for `clear()`'s predicate-based namespace invalidation.
            .support_invalidation_closures()
            .build();

        Self {
            inner: Arc::new(Inner {
                manifest: PluginManifest {
                    id: PLUGIN_ID.into(),
                    version: env!("CARGO_PKG_VERSION").into(),
                    name: "In-Memory Cache".into(),
                    plugin_class: PluginClass::Cache,
                    protocol_version: "1.0".into(),
                    license: None,
                    required_capabilities: Vec::new(),
                    tags: Vec::new(),
                    provides: Vec::new(),
                    provides_schemes: Vec::new(),
                    module_path_prefix: ::std::module_path!()
                        .split("::")
                        .next()
                        .unwrap_or("")
                        .to_owned(),
                    backend_profile: None,
                },
                cache,
            }),
        }
    }

    /// Flush moka's deferred maintenance (lazy invalidations). Used by `clear`
    /// and by tests that assert eviction synchronously.
    #[doc(hidden)]
    pub fn run_pending_tasks(&self) {
        self.inner.cache.run_pending_tasks();
    }
}

impl SyncCachePlugin for MemoryCache {
    fn manifest(&self) -> &PluginManifest {
        &self.inner.manifest
    }

    fn supported_namespaces(&self) -> Vec<String> {
        Vec::new()
    }

    fn serves_any_namespace(&self) -> bool {
        true
    }

    fn get(&self, ns: &str, key: &str) -> Option<Vec<u8>> {
        self.inner
            .cache
            .get(&(ns.to_owned(), key.to_owned()))
            .map(|e| e.bytes)
    }

    fn put(&self, ns: &str, key: &str, value: Vec<u8>, ttl_ms: u64) -> Result<(), CacheError> {
        self.inner.cache.insert(
            (ns.to_owned(), key.to_owned()),
            Entry {
                bytes: value,
                ttl: ttl_to_duration(ttl_ms),
            },
        );
        Ok(())
    }

    fn delete(&self, ns: &str, key: &str) {
        self.inner
            .cache
            .invalidate(&(ns.to_owned(), key.to_owned()));
    }

    fn clear(&self, ns: &str) -> Result<(), CacheError> {
        let ns_owned = ns.to_owned();
        self.inner
            .cache
            .invalidate_entries_if(move |k, _v| k.0 == ns_owned)
            .map_err(|e| CacheError::Backend {
                reason: format!("clear: {e}"),
            })?;
        // invalidate_entries_if is lazy — force the purge so a subsequent get
        // observes the eviction deterministically.
        self.inner.cache.run_pending_tasks();
        Ok(())
    }

    fn incr(&self, ns: &str, key: &str, by: i64, ttl_ms: u64) -> Result<i64, CacheError> {
        let ttl = ttl_to_duration(ttl_ms);
        // `and_upsert_with` serializes concurrent calls on the SAME key (moka
        // key-level lock), giving the atomicity the cache contract requires.
        let entry = self
            .inner
            .cache
            .entry((ns.to_owned(), key.to_owned()))
            .and_upsert_with(|maybe| {
                let cur = maybe
                    .map(|e| {
                        let v = e.into_value();
                        String::from_utf8_lossy(&v.bytes)
                            .trim()
                            .parse::<i64>()
                            .unwrap_or(0)
                    })
                    .unwrap_or(0);
                Entry {
                    bytes: (cur + by).to_string().into_bytes(),
                    ttl,
                }
            });
        let value = entry.into_value();
        String::from_utf8_lossy(&value.bytes)
            .trim()
            .parse::<i64>()
            .map_err(|e| CacheError::Backend {
                reason: format!("incr: counter decode failed: {e}"),
            })
    }

    fn shutdown(&self) {
        tracing::info!(plugin_id = PLUGIN_ID, "cache.memory: shutdown");
    }
}

#[cfg(any(feature = "cdylib-export", feature = "static-firstparty"))]
mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.cache.memory",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[],
    entities: [
        cache as entity {
            inner_name: "",
            plugin_type: MemoryCache,
            factory: |cfg: &str, _host: ::mcpg_plugin_sdk::HostHandle| MemoryCache::from_config_json(cfg),
        },
    ],
}

#[cfg(test)]
mod tests;
