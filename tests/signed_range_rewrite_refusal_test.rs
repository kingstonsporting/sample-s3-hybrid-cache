//! Focused regressions for the lower-level cache-repair APIs.
//!
//! The production incident (s3-proxy 2.8.0, 2026-09-03) reached a header-rewriting
//! repair path with a request whose `Range` was in SigV4 `SignedHeaders`, and S3
//! answered `SignatureDoesNotMatch`. The routing check in `http_proxy.rs` is one
//! guard; these tests pin the guards INSIDE the repair APIs, so a future caller that
//! skips the routing check still cannot rewrite a signed header.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use hyper::StatusCode;

use common::expired_fixture::{
    signed_authorization_without_range, signed_range_authorization, test_config, Fixture, SeedSpec,
};
use common::{StubResponse, StubS3Client};
use s3_proxy::cache::Range;
use s3_proxy::cache_types::ObjectMetadata;
use s3_proxy::compression::CompressionAlgorithm;
use s3_proxy::range_handler::RangeSpec;
use s3_proxy::ProxyError;

const HOST: &str = "s3.us-west-2.amazonaws.com";
const CACHE_KEY: &str = "test-bucket/signed-range-repair.bin";
const OBJECT_SIZE: u64 = 4096;

fn uri() -> hyper::Uri {
    "/test-bucket/signed-range-repair.bin".parse().unwrap()
}

fn headers(authorization: &str, range: Option<&str>) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert("authorization".to_string(), authorization.to_string());
    headers.insert("host".to_string(), HOST.to_string());
    if let Some(range) = range {
        headers.insert("range".to_string(), range.to_string());
    }
    headers
}

fn partial_content(len: usize) -> StubResponse {
    StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_body(Bytes::from(vec![b'B'; len]))
        .with_header("etag", "\"etag\"")
        .with_header(
            "content-range",
            format!("bytes 0-{}/{}", len - 1, OBJECT_SIZE),
        )
}

/// A cached extent whose `.bin` is missing, so any merge over it must fall back.
fn missing_cached_range(start: u64, end: u64) -> Range {
    Range {
        start,
        end,
        data: Vec::new(),
        etag: "\"etag\"".to_string(),
        last_modified: String::new(),
        compression_algorithm: CompressionAlgorithm::None,
    }
}

#[tokio::test]
async fn fetch_missing_ranges_refuses_to_rewrite_a_signed_range() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let stub = StubS3Client::new().with_default(partial_content(100));
    let s3 = stub.clone().into_trait_object();

    let result = fixture
        .range_handler
        .fetch_missing_ranges(
            CACHE_KEY,
            &[RangeSpec { start: 0, end: 99 }],
            &s3,
            HOST,
            &uri(),
            &headers(&signed_range_authorization(), Some("bytes=0-4095")),
            None,
            Duration::from_secs(1),
            1.0,
        )
        .await;

    assert!(
        matches!(
            result,
            Err(ProxyError::SignedHeaderRewriteRefused { ref header, .. }) if header == "range"
        ),
        "a signed Range must be refused, got {:?}",
        result.map(|r| r.len())
    );
    assert!(
        stub.captured().is_empty(),
        "no upstream request may be made with a rewritten signed header: {:?}",
        stub.captured()
    );
}

#[tokio::test]
async fn fetch_missing_ranges_still_rewrites_when_range_is_not_signed() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let stub = StubS3Client::new().with_default(partial_content(100));
    let s3 = stub.clone().into_trait_object();

    let fetched = fixture
        .range_handler
        .fetch_missing_ranges(
            CACHE_KEY,
            &[RangeSpec { start: 0, end: 99 }],
            &s3,
            HOST,
            &uri(),
            &headers(&signed_authorization_without_range(), None),
            None,
            Duration::from_secs(1),
            1.0,
        )
        .await
        .expect("an unsigned Range may be synthesised");

    assert_eq!(fetched.len(), 1);
    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].headers.get("Range").map(String::as_str),
        Some("bytes=0-99")
    );
}

#[tokio::test]
async fn fallback_fetch_forwards_a_signed_suffix_range_verbatim() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let stub = StubS3Client::new().with_default(
        StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
            .with_body(Bytes::from(vec![b'B'; 512]))
            .with_header("etag", "\"etag\"")
            .with_header("content-range", format!("bytes 3584-4095/{}", OBJECT_SIZE)),
    );
    let s3 = stub.clone().into_trait_object();
    let requested = RangeSpec {
        start: 3584,
        end: 4095,
    };

    let merged = fixture
        .range_handler
        .merge_ranges_with_fallback(
            CACHE_KEY,
            &requested,
            &[missing_cached_range(3584, 4095)],
            &[],
            &s3,
            HOST,
            &uri(),
            &headers(&signed_range_authorization(), Some("bytes=-512")),
            None,
        )
        .await
        .expect("fallback over a missing range must succeed");

    assert_eq!(merged.data.len(), 512);
    let captured = stub.captured();
    assert_eq!(
        captured.len(),
        1,
        "exactly one fallback fetch: {:?}",
        captured
    );
    assert_eq!(
        captured[0].headers.get("range").map(String::as_str),
        Some("bytes=-512"),
        "the client's signed suffix Range must reach S3 byte-for-byte"
    );
}

#[tokio::test]
async fn fallback_fetch_refuses_a_signed_range_that_differs_from_the_client_range() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let stub = StubS3Client::new().with_default(partial_content(100));
    let s3 = stub.clone().into_trait_object();

    let result = fixture
        .range_handler
        .merge_ranges_with_fallback(
            CACHE_KEY,
            &RangeSpec { start: 0, end: 99 },
            &[missing_cached_range(0, 99)],
            &[],
            &s3,
            HOST,
            &uri(),
            &headers(&signed_range_authorization(), Some("bytes=0-4095")),
            None,
        )
        .await;

    assert!(
        matches!(result, Err(ProxyError::SignedHeaderRewriteRefused { .. })),
        "a sub-range fetch under a different signed Range must be refused"
    );
    assert!(stub.captured().is_empty(), "{:?}", stub.captured());
}

#[tokio::test]
async fn fallback_fetch_normalises_the_range_when_it_is_not_signed() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let stub = StubS3Client::new().with_default(partial_content(100));
    let s3 = stub.clone().into_trait_object();

    fixture
        .range_handler
        .merge_ranges_with_fallback(
            CACHE_KEY,
            &RangeSpec { start: 0, end: 99 },
            &[missing_cached_range(0, 99)],
            &[],
            &s3,
            HOST,
            &uri(),
            &headers(&signed_authorization_without_range(), Some("bytes=0-4095")),
            None,
        )
        .await
        .expect("unsigned fallback succeeds");

    let captured = stub.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].headers.get("range").map(String::as_str),
        Some("bytes=0-99")
    );
}

#[tokio::test]
async fn a_range_without_an_etag_never_displaces_metadata_that_has_one() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    fixture.seed(
        CACHE_KEY,
        &SeedSpec::expired(vec![(0, 4095)], OBJECT_SIZE, "\"good-etag\""),
    );
    let before = fixture.read_meta(CACHE_KEY).expect("seeded metadata");
    assert_eq!(before.object_metadata.etag, "\"good-etag\"");
    assert!(fixture.bin_path(CACHE_KEY, 0, 4095).exists());

    // What the old recovery path stored: an upstream error body with no ETag.
    let error_body = b"<Error><Code>SignatureDoesNotMatch</Code></Error>".to_vec();
    let result = fixture
        .range_handler
        .store_range_new_storage(
            CACHE_KEY,
            0,
            (error_body.len() - 1) as u64,
            &error_body,
            ObjectMetadata::default(),
            Duration::from_secs(60),
            false,
        )
        .await;

    assert!(result.is_err(), "an ETag-less range must be refused");
    let after = fixture.read_meta(CACHE_KEY).expect("metadata survives");
    assert_eq!(after.object_metadata.etag, "\"good-etag\"");
    assert_eq!(after.ranges.len(), 1);
    assert!(
        fixture.bin_path(CACHE_KEY, 0, 4095).exists(),
        "the valid range file must not be deleted"
    );
}

#[tokio::test]
async fn invalidation_paths_drop_the_ram_metadata_snapshot() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    fixture.seed(
        CACHE_KEY,
        &SeedSpec::expired(vec![(0, 4095)], OBJECT_SIZE, "\"etag\""),
    );
    let metadata_cache = fixture.cache_manager.get_metadata_cache();

    // Populate the RAM snapshot the way the GET path does.
    fixture
        .cache_manager
        .get_metadata_cached(CACHE_KEY)
        .await
        .expect("read")
        .expect("seeded");
    assert!(metadata_cache.peek(CACHE_KEY).await.is_some());

    fixture
        .cache_manager
        .invalidate_cache_hierarchy(CACHE_KEY)
        .await
        .expect("invalidate");
    assert!(
        metadata_cache.peek(CACHE_KEY).await.is_none(),
        "invalidate_cache_hierarchy must drop the RAM metadata snapshot"
    );

    fixture.seed(
        CACHE_KEY,
        &SeedSpec::expired(vec![(0, 2047), (2048, 4095)], OBJECT_SIZE, "\"etag\""),
    );
    fixture
        .cache_manager
        .get_metadata_cached(CACHE_KEY)
        .await
        .expect("read")
        .expect("seeded");
    assert!(metadata_cache.peek(CACHE_KEY).await.is_some());

    fixture
        .cache_manager
        .evict_inconsistent_range(CACHE_KEY, 2048, 4095, "test")
        .await;
    assert!(
        metadata_cache.peek(CACHE_KEY).await.is_none(),
        "evicting an inconsistent range must drop the RAM metadata snapshot"
    );
    let remaining = fixture.read_meta(CACHE_KEY).expect("metadata survives");
    assert_eq!(remaining.ranges.len(), 1);
    assert_eq!(remaining.ranges[0].start, 0);
    assert!(!fixture.bin_path(CACHE_KEY, 2048, 4095).exists());
    assert!(fixture.bin_path(CACHE_KEY, 0, 2047).exists());
    let _ = SystemTime::now();
    let _: Arc<_> = metadata_cache;
}

/// The one legitimate short body: a request past the end of the object, which S3
/// answers with fewer bytes and a `Content-Range` naming the true last byte. That
/// clamp must keep working; the same short body without S3's confirmation must not
/// be cached as a "valid" shorter range.
#[tokio::test]
async fn a_short_body_is_clamped_only_when_content_range_confirms_the_object_end() {
    let fixture = Fixture::new(test_config(OBJECT_SIZE)).await;
    let body = vec![b'A'; 10];

    let mut confirmed = HashMap::new();
    confirmed.insert("etag".to_string(), "\"etag\"".to_string());
    confirmed.insert("content-range".to_string(), "bytes 0-9/10".to_string());
    fixture
        .range_handler
        .store_range_new_storage(
            "test-bucket/ten-bytes-confirmed.bin",
            0,
            99,
            &body,
            ObjectMetadata::new_with_headers(
                "\"etag\"".to_string(),
                String::new(),
                10,
                None,
                confirmed,
            ),
            Duration::from_secs(60),
            false,
        )
        .await
        .expect("a beyond-EOF request confirmed by Content-Range is cached clamped");
    // Range stores are journal-only in shared-storage mode; the `.meta` appears
    // once the consolidator folds the entry in, as it does in production.
    fixture
        .cache_manager
        .get_journal_consolidator()
        .await
        .expect("shared-storage consolidator")
        .run_consolidation_cycle()
        .await
        .expect("consolidation cycle");
    let meta = fixture
        .read_meta("test-bucket/ten-bytes-confirmed.bin")
        .expect("clamped metadata");
    assert_eq!(meta.ranges.len(), 1);
    assert_eq!((meta.ranges[0].start, meta.ranges[0].end), (0, 9));
    assert_eq!(meta.ranges[0].uncompressed_size, 10);

    let mut unconfirmed = HashMap::new();
    unconfirmed.insert("etag".to_string(), "\"etag\"".to_string());
    let result = fixture
        .range_handler
        .store_range_new_storage(
            "test-bucket/ten-bytes-unconfirmed.bin",
            0,
            99,
            &body,
            ObjectMetadata::new_with_headers(
                "\"etag\"".to_string(),
                String::new(),
                100,
                None,
                unconfirmed,
            ),
            Duration::from_secs(60),
            false,
        )
        .await;
    assert!(
        result.is_err(),
        "a short body without a confirming Content-Range must be refused, not clamped"
    );
    assert!(
        fixture
            .read_meta("test-bucket/ten-bytes-unconfirmed.bin")
            .is_none(),
        "nothing may be published for the refused store"
    );
}
