//! A draft is one layout's placement: what it does not read is refused.

use axum::http::StatusCode;
use serde_json::json;

use super::{call, draft, fixture_catalog, router, scratch};

/// The composer places a graph in one layout. A draft that carries more,
/// such as a spec's `[layout]` tables, is refused, rather than saved with
/// its top-level segments and the layouts dropped.
#[tokio::test]
async fn a_draft_with_layouts_is_refused_not_dropped() {
    let temp = scratch("layouts");
    let dir = temp.path();
    let router = router(dir, fixture_catalog());
    let plain = draft(
        "regproto::Words",
        "Engine",
        json!({ "all": ["A", "Engine"] }),
    );
    let (status, bytes) = call(&router, "POST", "/api/export", Some(plain.clone())).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    let mut layouts = plain;
    layouts["layout"] = json!({
        "east": { "segment": { "all": { "nodes": ["A", "Engine"] } } },
        "west": { "segment": { "front": { "nodes": ["A"] }, "engine": { "nodes": ["Engine"] } } },
    });
    for path in ["/api/check", "/api/export", "/api/deploy"] {
        let (status, bytes) = call(&router, "POST", path, Some(layouts.clone())).await;
        let body = String::from_utf8_lossy(&bytes);
        assert!(!status.is_success(), "{path}: {status} {body}");
        assert!(body.contains("unknown field `layout`"), "{path}: {body}");
    }
}
