use std::sync::Arc;
use std::thread;
use std::time::Duration;

use mcpg_plugin_protocol::PluginClass;
use mcpg_plugin_sdk::ffi::SyncCachePlugin;

use super::{MemoryCache, PLUGIN_ID};

const DESCRIPTOR: &str = include_str!("../plugin.yaml");

fn build(cfg: &str) -> MemoryCache {
    MemoryCache::from_config_json(cfg)
}

fn cache() -> MemoryCache {
    build(r#"{"max_capacity": 1000}"#)
}

#[test]
fn factory_parses_minimal_config() {
    let _ = build("{}");
    let _ = build(r#"{"max_capacity": 100}"#);
}

#[test]
#[should_panic(expected = "config parse failed")]
fn factory_panics_on_unparseable_config() {
    let _ = build("not-json");
}

#[test]
#[should_panic(expected = "config parse failed")]
fn unknown_field_is_rejected() {
    let _ = build(r#"{"bogus": 1}"#);
}

#[test]
#[should_panic(expected = "config parse failed")]
fn zero_max_capacity_is_rejected() {
    let _ = build(r#"{"max_capacity": 0}"#);
}

#[test]
fn manifest_carries_class_cache() {
    let c = cache();
    let m = SyncCachePlugin::manifest(&c);
    assert_eq!(m.id, PLUGIN_ID);
    assert_eq!(m.plugin_class, PluginClass::Cache);
    assert_eq!(m.protocol_version, "1.0");
    assert!(m.required_capabilities.is_empty());
}

#[test]
fn serves_any_namespace_and_empty_supported() {
    let c = cache();
    assert!(c.serves_any_namespace());
    assert!(c.supported_namespaces().is_empty());
}

#[test]
fn descriptor_yaml_is_well_formed() {
    assert!(DESCRIPTOR.contains("id: dev.mcpg.cache.memory"));
    assert!(DESCRIPTOR.contains("class: cache"));
    assert!(DESCRIPTOR.contains("runtime: native-cdylib-v1"));
    assert!(!DESCRIPTOR.contains("network_outbound"));
}

#[test]
fn put_get_roundtrip() {
    let c = cache();
    c.put("ns", "k", b"v".to_vec(), 60_000).unwrap();
    assert_eq!(c.get("ns", "k"), Some(b"v".to_vec()));
}

#[test]
fn get_missing_returns_none() {
    let c = cache();
    assert_eq!(c.get("ns", "absent"), None);
}

#[test]
fn get_after_ttl_expiry() {
    let c = cache();
    c.put("ns", "k", b"v".to_vec(), 1).unwrap();
    thread::sleep(Duration::from_millis(30));
    c.run_pending_tasks();
    assert_eq!(c.get("ns", "k"), None);
}

#[test]
fn delete_removes_key_and_missing_is_noop() {
    let c = cache();
    c.put("ns", "k", b"v".to_vec(), 60_000).unwrap();
    c.delete("ns", "k");
    assert_eq!(c.get("ns", "k"), None);
    // No panic on deleting an absent key.
    c.delete("ns", "absent");
}

#[test]
fn clear_namespace_only() {
    let c = cache();
    c.put("a", "k1", b"1".to_vec(), 60_000).unwrap();
    c.put("a", "k2", b"2".to_vec(), 60_000).unwrap();
    c.put("b", "k3", b"3".to_vec(), 60_000).unwrap();
    c.clear("a").unwrap();
    assert_eq!(c.get("a", "k1"), None);
    assert_eq!(c.get("a", "k2"), None);
    assert_eq!(c.get("b", "k3"), Some(b"3".to_vec()));
}

#[test]
fn incr_from_missing_initializes_to_by() {
    let c = cache();
    assert_eq!(c.incr("ns", "ctr", 5, 60_000).unwrap(), 5);
}

#[test]
fn incr_accumulates_and_handles_negative() {
    let c = cache();
    assert_eq!(c.incr("ns", "ctr", 5, 60_000).unwrap(), 5);
    assert_eq!(c.incr("ns", "ctr", 3, 60_000).unwrap(), 8);
    assert_eq!(c.incr("ns", "ctr", -2, 60_000).unwrap(), 6);
    // The stored value reads back as decimal-ASCII bytes.
    assert_eq!(c.get("ns", "ctr"), Some(b"6".to_vec()));
}

#[test]
fn incr_atomic_under_threads() {
    let c = Arc::new(cache());
    const THREADS: i64 = 8;
    const ITERS: i64 = 500;
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let c = Arc::clone(&c);
            thread::spawn(move || {
                for _ in 0..ITERS {
                    c.incr("ns", "shared", 1, 60_000).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(c.incr("ns", "shared", 0, 60_000).unwrap(), THREADS * ITERS);
}

#[test]
fn put_never_errs() {
    let c = cache();
    assert!(c.put("ns", "k", b"v".to_vec(), 0).is_ok());
}
