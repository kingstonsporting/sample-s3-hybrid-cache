//! End-to-end regression for a GET immediately following a write-through PUT.
//!
//! S3's PutObject response does not include Last-Modified, so the write cache
//! initially has a complete body and ETag but no Last-Modified. The first GET
//! must validate that representation with S3, learn Last-Modified from the 304
//! response, persist it, and include it on the client-facing cached response.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    conditional_requests, proxy_get_with_auth, signed_authorization_without_range,
    test_config_full_object_checks_with_get_ttl, Fixture,
};
use common::{StubResponse, StubS3Client};
use s3_proxy::cache_types::CacheMetadata;
use s3_proxy::config::Config;

const BODY: &[u8] = b"write-through body";
const ETAG: &str = "\"write-through-etag\"";
const LAST_MODIFIED: &str = "Thu, 03 Sep 2026 10:11:12 GMT";
const CACHE_KEY: &str = "test-bucket/write-through.bin";
const REQUEST_PATH: &str = "/test-bucket/write-through.bin";

async fn put_through_write_cache(
    fixture: &Fixture,
    cache_key: &str,
    body: &[u8],
    etag: &str,
    last_modified: &str,
) {
    common::put_through_write_cache(
        &fixture.cache_manager,
        cache_key,
        body,
        HashMap::from([(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        )]),
        CacheMetadata {
            etag: etag.to_string(),
            last_modified: last_modified.to_string(),
            content_length: body.len() as u64,
            part_number: None,
            cache_control: None,
            access_count: 0,
            last_accessed: SystemTime::now(),
        },
        HashMap::new(),
    )
    .await
    .expect("store write-through PUT");
}

#[tokio::test]
async fn first_get_after_write_through_put_returns_and_persists_last_modified() {
    let config =
        test_config_full_object_checks_with_get_ttl(BODY.len() as u64, Duration::from_secs(3600));
    let fixture = Fixture::new(config).await;

    put_through_write_cache(&fixture, CACHE_KEY, BODY, ETAG, "").await;

    let before = fixture.read_meta(CACHE_KEY).expect("write cache metadata");
    assert!(before.object_metadata.is_write_cached);
    assert_eq!(before.object_metadata.last_modified, "");

    let unchanged = StubResponse::not_modified()
        .with_header("etag", ETAG)
        .with_header("last-modified", LAST_MODIFIED);
    let stub = StubS3Client::new()
        .with_response_for_etag(ETAG, unchanged)
        .with_default(
            StubResponse::with_status(StatusCode::OK)
                .with_body(Bytes::from_static(b"unexpected upstream body"))
                .with_header("etag", "\"changed-etag\"")
                .with_header("last-modified", "Fri, 04 Sep 2026 10:11:12 GMT"),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, headers, body) = proxy_get_with_auth(
        server.addr,
        REQUEST_PATH,
        &signed_authorization_without_range(),
        &[],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, BODY);
    assert_eq!(
        headers
            .get("last-modified")
            .and_then(|value| value.to_str().ok()),
        Some(LAST_MODIFIED),
        "the cached GET response must expose S3's authoritative Last-Modified"
    );

    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);
    assert_eq!(
        conditionals.len(),
        1,
        "an incomplete write-cache entry must be validated once; captured: {captured:#?}"
    );
    assert_eq!(conditionals[0].if_none_match(), Some(ETAG));
    assert_eq!(conditionals[0].if_modified_since(), None);

    let after = fixture.read_meta(CACHE_KEY).expect("revalidated metadata");
    assert!(!after.object_metadata.is_write_cached);
    assert_eq!(after.object_metadata.last_modified, LAST_MODIFIED);
    assert_eq!(
        after
            .object_metadata
            .response_headers
            .get("last-modified")
            .map(String::as_str),
        Some(LAST_MODIFIED)
    );

    let (second_status, second_headers, second_body) = proxy_get_with_auth(
        server.addr,
        REQUEST_PATH,
        &signed_authorization_without_range(),
        &[],
    )
    .await;
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(second_body, BODY);
    assert_eq!(
        second_headers
            .get("last-modified")
            .and_then(|value| value.to_str().ok()),
        Some(LAST_MODIFIED)
    );
    assert_eq!(
        stub.captured().len(),
        1,
        "complete metadata inside its GET TTL must not revalidate again"
    );
}

#[tokio::test]
async fn first_range_get_after_write_through_put_returns_last_modified() {
    let mut config = Config::default();
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.full_object_check_threshold = 1;
    config.cache.get_ttl = Duration::from_secs(3600);
    let fixture = Fixture::new(Arc::new(config)).await;
    let cache_key = "test-bucket/write-through-range.bin";
    let request_path = "/test-bucket/write-through-range.bin";

    put_through_write_cache(&fixture, cache_key, BODY, ETAG, "").await;

    let unchanged = StubResponse::not_modified()
        .with_header("etag", ETAG)
        .with_header("last-modified", LAST_MODIFIED);
    let stub = StubS3Client::new()
        .with_response_for_etag(ETAG, unchanged)
        .with_default(
            StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
                .with_body(Bytes::from_static(b"wrong"))
                .with_header("content-range", format!("bytes 0-4/{}", BODY.len()))
                .with_header("etag", "\"changed-etag\""),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;

    let (status, headers, body) = proxy_get_with_auth(
        server.addr,
        request_path,
        &common::expired_fixture::signed_range_authorization(),
        &[("range", "bytes=0-4")],
    )
    .await;

    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(body, &BODY[0..5]);
    assert_eq!(
        headers
            .get("last-modified")
            .and_then(|value| value.to_str().ok()),
        Some(LAST_MODIFIED)
    );

    let captured = stub.captured();
    let conditionals = conditional_requests(&captured);
    assert_eq!(conditionals.len(), 1, "captured: {captured:#?}");
    assert_eq!(conditionals[0].if_none_match(), Some(ETAG));
    assert_eq!(
        conditionals[0].headers.get("range").map(String::as_str),
        Some("bytes=0-4")
    );

    let metadata = fixture.read_meta(cache_key).expect("revalidated metadata");
    assert_eq!(metadata.object_metadata.last_modified, LAST_MODIFIED);
}

#[tokio::test]
async fn revalidation_replaces_an_existing_stale_last_modified() {
    const OLD_LAST_MODIFIED: &str = "Wed, 02 Sep 2026 10:11:12 GMT";

    let config = test_config_full_object_checks_with_get_ttl(BODY.len() as u64, Duration::ZERO);
    let fixture = Fixture::new(config).await;
    put_through_write_cache(&fixture, CACHE_KEY, BODY, ETAG, OLD_LAST_MODIFIED).await;

    let unchanged = StubResponse::not_modified()
        .with_header("etag", ETAG)
        .with_header("last-modified", LAST_MODIFIED);
    let stub = StubS3Client::new().with_response_for_etag(ETAG, unchanged);
    let server = fixture.spawn_proxy(stub.into_trait_object()).await;

    let (status, headers, body) = proxy_get_with_auth(
        server.addr,
        REQUEST_PATH,
        &signed_authorization_without_range(),
        &[],
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, BODY);
    assert_eq!(
        headers
            .get("last-modified")
            .and_then(|value| value.to_str().ok()),
        Some(LAST_MODIFIED)
    );
    assert_eq!(
        fixture
            .read_meta(CACHE_KEY)
            .expect("revalidated metadata")
            .object_metadata
            .effective_last_modified(),
        Some(LAST_MODIFIED)
    );
}

#[tokio::test]
async fn concurrent_replacement_during_revalidation_falls_back_to_s3() {
    const REPLACEMENT_BODY: &[u8] = b"replacement cached body";
    const REPLACEMENT_ETAG: &str = "\"replacement-etag\"";
    const UPSTREAM_BODY: &[u8] = b"authoritative upstream body";

    let mut config =
        (*test_config_full_object_checks_with_get_ttl(BODY.len() as u64, Duration::ZERO)).clone();
    config.cache.download_coordination.enabled = false;
    let fixture = Fixture::new(Arc::new(config)).await;
    put_through_write_cache(&fixture, CACHE_KEY, BODY, ETAG, LAST_MODIFIED).await;

    let delayed_not_modified = StubResponse::not_modified()
        .with_header("etag", ETAG)
        .with_header("last-modified", LAST_MODIFIED)
        .with_delay(Duration::from_millis(100));
    let stub = StubS3Client::new()
        .with_response_for_etag(ETAG, delayed_not_modified)
        .with_default(
            StubResponse::with_status(StatusCode::OK)
                .with_body(Bytes::from_static(UPSTREAM_BODY))
                .with_header("etag", REPLACEMENT_ETAG)
                .with_header("last-modified", "Fri, 04 Sep 2026 10:11:12 GMT")
                .with_header("content-length", UPSTREAM_BODY.len().to_string()),
        );
    let server = fixture.spawn_proxy(stub.clone().into_trait_object()).await;
    let addr = server.addr;

    let request = tokio::spawn(async move {
        proxy_get_with_auth(
            addr,
            REQUEST_PATH,
            &signed_authorization_without_range(),
            &[],
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        while stub.captured().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("conditional request reached S3 stub");

    put_through_write_cache(
        &fixture,
        CACHE_KEY,
        REPLACEMENT_BODY,
        REPLACEMENT_ETAG,
        "Fri, 04 Sep 2026 10:11:12 GMT",
    )
    .await;

    let (status, headers, body) = request.await.expect("proxy request task");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, UPSTREAM_BODY);
    assert_eq!(
        headers.get("etag").and_then(|value| value.to_str().ok()),
        Some(REPLACEMENT_ETAG)
    );

    let captured = stub.captured();
    assert_eq!(captured.len(), 2, "captured: {captured:#?}");
    assert_eq!(captured[0].if_none_match(), Some(ETAG));
    assert_eq!(captured[1].if_none_match(), None);
}
