//! End-to-end regressions for two ways the cache returned bytes that were not the
//! object S3 held (s3-proxy at fork `fd7942c`, production 2026-10-15).
//!
//! # Production failure
//!
//! A Prefect flow rewrote a ~588 KB Parquet file directly to S3 every five minutes.
//! Other readers on the worker had left PyArrow-style ranged reads of one version in
//! the node cache (its 64 KiB footer plus column chunks, 24% of the object). After
//! the next overwrite, a plain `get_object` through the cache got `206` with
//! `Content-Range: bytes 0-587622/587722`: the first 587,623 bytes of the new
//! version, so `pd.read_parquet` failed with "Parquet magic bytes not found in
//! footer". The full-object GET had synthesized `Range: bytes=0-{cached_size-1}` to
//! fetch only the gaps; the gap fetch carried a proxy-injected `If-Match`, S3
//! answered `412`, and the retry dropped the `If-Match` but kept the stale Range.
//!
//! # Found while reproducing it
//!
//! A cold `Range: bytes=-N` cannot be resolved without the object size, so it was
//! forwarded through the whole-object path, which cached S3's `206` body at
//! `0..N-1`. Every later read of the object's head that the cache covered returned
//! the footer instead ("Deserializing page header failed").
//!
//! # Topology
//!
//! The production worker configuration from ksg-infra
//! `scripts/s3-hybrid-cache/install.sh` (`get_ttl: 0s`, `head_ttl: 0s`, write cache
//! and RAM range cache off), the proxy as `HttpProxy::new` wires it with a real
//! `S3Client`, a background consolidation loop, and a mock S3 that honours `Range`,
//! `If-Match` and `If-None-Match` the way S3 does. Objects are overwritten in the
//! mock directly, as the producer bypassed the cache.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::net::TcpListener;

use common::expired_fixture::{
    proxy_get_with_auth, signed_authorization_without_range, signed_range_authorization,
};
use common::graded_fixture::SplitMix64;
use s3_proxy::cache::CacheManager;
use s3_proxy::cache_types::NewCacheMetadata;
use s3_proxy::config::{Config, UpstreamOverrideConfig, UpstreamScheme};
use s3_proxy::http_proxy::HttpProxy;

const BUCKET: &str = "ksg-data-e2e";
/// PyArrow's default footer read.
const FOOTER: u64 = 64 * 1024;

fn object_bytes(len: usize, seed: u64) -> Bytes {
    let mut rng = SplitMix64::new(seed);
    (0..len).map(|_| rng.next_u64() as u8).collect()
}

fn slice(data: &Bytes, start: u64, end: u64) -> Bytes {
    data.slice(start as usize..=end as usize)
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|v| v.to_str().ok())
}

// ─────────────────────────────────────────────────────────────────────────────
// Mock S3
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct StoredObject {
    data: Bytes,
    etag: String,
    last_modified: SystemTime,
}

#[derive(Debug, Clone)]
struct UpstreamRequest {
    range: Option<String>,
    if_match: Option<String>,
    status: u16,
}

#[derive(Default)]
struct MockState {
    objects: HashMap<String, StoredObject>,
    versions: u64,
    log: Vec<UpstreamRequest>,
}

#[derive(Clone, Default)]
struct MockS3 {
    state: Arc<Mutex<MockState>>,
}

impl MockS3 {
    /// Write a new version directly, as a producer that bypasses the cache does. Each
    /// version is a second newer than the last: the cache replaces a cached version's
    /// identity only for a strictly newer Last-Modified.
    fn put(&self, key: &str, data: Bytes) -> String {
        let mut state = self.state.lock().unwrap();
        state.versions += 1;
        let etag = format!("\"v{}-{}\"", state.versions, data.len());
        let last_modified =
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_792_000_000 + state.versions);
        state.objects.insert(
            format!("/{}/{}", BUCKET, key),
            StoredObject {
                data,
                etag: etag.clone(),
                last_modified,
            },
        );
        etag
    }

    fn log(&self) -> Vec<UpstreamRequest> {
        self.state.lock().unwrap().log.clone()
    }

    async fn handle(self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let headers = req.headers();
        let response = self.respond(req.method(), req.uri().path(), headers);
        self.state.lock().unwrap().log.push(UpstreamRequest {
            range: header(headers, "range").map(str::to_string),
            if_match: header(headers, "if-match").map(str::to_string),
            status: response.status().as_u16(),
        });
        Ok(response)
    }

    fn respond(&self, method: &Method, path: &str, headers: &HeaderMap) -> Response<Full<Bytes>> {
        assert_eq!(method, Method::GET, "the scenarios only send GETs upstream");
        let Some(object) = self.state.lock().unwrap().objects.get(path).cloned() else {
            return status_only(StatusCode::NOT_FOUND);
        };
        let total = object.data.len() as u64;
        let base = |status: StatusCode| {
            Response::builder()
                .status(status)
                .header("etag", object.etag.clone())
                .header(
                    "last-modified",
                    httpdate::fmt_http_date(object.last_modified),
                )
                .header("accept-ranges", "bytes")
                .header("content-type", "application/octet-stream")
        };

        // S3 evaluates If-Match before If-None-Match, and both before Range.
        if header(headers, "if-match").is_some_and(|etag| etag != object.etag) {
            return status_only(StatusCode::PRECONDITION_FAILED);
        }
        if header(headers, "if-none-match") == Some(object.etag.as_str()) {
            return base(StatusCode::NOT_MODIFIED)
                .body(Full::new(Bytes::new()))
                .unwrap();
        }
        let Some(range) = header(headers, "range") else {
            return base(StatusCode::OK)
                .header("content-length", total.to_string())
                .body(Full::new(object.data))
                .unwrap();
        };
        let (start, end) = match range.trim_start_matches("bytes=").split_once('-') {
            Some(("", suffix)) => (total.saturating_sub(suffix.parse().unwrap()), total - 1),
            Some((start, "")) => (start.parse().unwrap(), total - 1),
            Some((start, end)) => (
                start.parse().unwrap(),
                end.parse::<u64>().unwrap().min(total - 1),
            ),
            None => panic!("unparseable Range {range}"),
        };
        base(StatusCode::PARTIAL_CONTENT)
            .header("content-length", (end - start + 1).to_string())
            .header("content-range", format!("bytes {start}-{end}/{total}"))
            .body(Full::new(slice(&object.data, start, end)))
            .unwrap()
    }
}

fn status_only(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Proxy under test
// ─────────────────────────────────────────────────────────────────────────────

struct Harness {
    _temp_dir: TempDir,
    mock: MockS3,
    mock_addr: String,
    proxy_addr: SocketAddr,
    cache_manager: Arc<CacheManager>,
}

struct ClientResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Harness {
    async fn start(get_ttl: Duration) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mock = MockS3::default();
        let mock_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock s3");
        let mock_addr = mock_listener.local_addr().unwrap().to_string();
        {
            let mock = mock.clone();
            tokio::spawn(async move {
                while let Ok((stream, _)) = mock_listener.accept().await {
                    let mock = mock.clone();
                    tokio::spawn(async move {
                        let service = service_fn(move |req| mock.clone().handle(req));
                        let _ = http1::Builder::new()
                            .serve_connection(TokioIo::new(stream), service)
                            .await;
                    });
                }
            });
        }

        let temp_dir = TempDir::new().expect("tempdir");
        let mut config = Config::default();
        config.cache.cache_dir = temp_dir.path().to_path_buf();
        config.cache.write_cache_enabled = false;
        config.cache.get_ttl = get_ttl;
        config.cache.head_ttl = Duration::ZERO;
        config.cache.ram_cache_enabled = false;
        config.cache.actively_remove_cached_data = false;
        config.connection_pool.upstream_overrides.insert(
            mock_addr.clone(),
            UpstreamOverrideConfig {
                scheme: UpstreamScheme::Http,
                validate_tls: true,
            },
        );
        let config = Arc::new(config);
        let proxy =
            Arc::new(HttpProxy::new("127.0.0.1:0".parse().unwrap(), config.clone()).unwrap());
        let cache_manager = proxy.get_cache_manager();
        cache_manager.initialize().await.expect("cache init");
        proxy
            .get_range_handler()
            .get_disk_cache_manager()
            .write()
            .await
            .initialize()
            .await
            .expect("disk cache init");
        if let Some(consolidator) = cache_manager.get_journal_consolidator().await {
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                loop {
                    interval.tick().await;
                    let _ = consolidator.run_consolidation_cycle().await;
                }
            });
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let proxy_addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, peer)) = listener.accept().await {
                let proxy = Arc::clone(&proxy);
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let s3_client = proxy.get_s3_client();
                        let inflight_ledger = s3_client.get_inflight_ledger();
                        HttpProxy::handle_request(
                            req,
                            peer,
                            Arc::clone(&config),
                            proxy.get_cache_manager(),
                            s3_client,
                            proxy.get_range_handler(),
                            proxy.get_request_semaphore(),
                            None,
                            None,
                            proxy.get_inflight_tracker(),
                            None,
                            None,
                            None,
                            inflight_ledger,
                        )
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        Self {
            _temp_dir: temp_dir,
            mock,
            mock_addr,
            proxy_addr,
            cache_manager,
        }
    }

    async fn get(&self, key: &str, authorization: &str, extra: &[(&str, &str)]) -> ClientResponse {
        let mut headers = vec![("host", self.mock_addr.as_str())];
        headers.extend_from_slice(extra);
        let (status, headers, body) = proxy_get_with_auth(
            self.proxy_addr,
            &format!("/{}/{}", BUCKET, key),
            authorization,
            &headers,
        )
        .await;
        ClientResponse {
            status,
            headers,
            body,
        }
    }

    /// A signed ranged read, as Botocore and PyArrow send it.
    async fn get_range(&self, key: &str, range: &str) -> ClientResponse {
        self.get(key, &signed_range_authorization(), &[("range", range)])
            .await
    }

    /// The cached extents of the object, once consolidation has published exactly
    /// `count` of them.
    async fn wait_for_extents(&self, key: &str, count: usize) -> Vec<(u64, u64)> {
        let path = self
            .cache_manager
            .get_new_metadata_file_path(&format!("{}/{}", BUCKET, key));
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let extents = std::fs::read_to_string(&path)
                .ok()
                .and_then(|json| serde_json::from_str::<NewCacheMetadata>(&json).ok())
                .map(|meta| {
                    let mut extents: Vec<_> =
                        meta.ranges.iter().map(|r| (r.start, r.end)).collect();
                    extents.sort_unstable();
                    extents
                })
                .unwrap_or_default();
            if extents.len() == count || tokio::time::Instant::now() > deadline {
                return extents;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenarios
// ─────────────────────────────────────────────────────────────────────────────

/// Leave PyArrow's reads of `v0` in the cache: the 64 KiB footer and one column
/// chunk, 138,786 bytes as on the production node.
async fn cache_pyarrow_reads(harness: &Harness, key: &str, v0: &Bytes) {
    let last = v0.len() as u64 - 1;
    let footer = last + 1 - FOOTER;
    let read = harness
        .get_range(key, &format!("bytes={footer}-{last}"))
        .await;
    assert!(read.body == slice(v0, footer, last));
    let read = harness.get_range(key, "bytes=4-73253").await;
    assert!(read.body == slice(v0, 4, 73253));
    assert_eq!(
        harness.wait_for_extents(key, 2).await,
        vec![(4, 73253), (footer, last)],
        "PRECONDITION: both reads must be cached so the GET sees partial coverage"
    );
}

async fn plain_get_after_overwrite(new_len: Option<usize>) {
    let harness = Harness::start(Duration::ZERO).await;
    let key = "metadata/code=H/source=tab/year=2026/month=10/2026-10-16.parquet";
    let v0 = object_bytes(587_623, 1);
    let v0_etag = harness.mock.put(key, v0.clone());
    cache_pyarrow_reads(&harness, key, &v0).await;

    let (current, etag) = match new_len {
        Some(len) => {
            let v1 = object_bytes(len, 2);
            let etag = harness.mock.put(key, v1.clone());
            (v1, etag)
        }
        None => (v0, v0_etag),
    };

    let upstream_before = harness.mock.log().len();
    let response = harness
        .get(key, &signed_authorization_without_range(), &[])
        .await;
    let upstream = harness.mock.log().split_off(upstream_before);
    assert_eq!(
        (response.status, response.body.len()),
        (StatusCode::OK, current.len()),
        "a GET without Range must be answered with 200 and the whole current object; \
         Content-Range {:?}; upstream: {upstream:?}",
        header(&response.headers, "content-range")
    );
    assert_eq!(header(&response.headers, "content-range"), None);
    assert_eq!(header(&response.headers, "etag"), Some(etag.as_str()));
    assert!(
        response.body == current,
        "the body must be the current object's bytes"
    );
    assert!(
        upstream
            .iter()
            .all(|r| r.range.is_none() && r.if_match.is_none() && r.status == 200),
        "the client's own request must be forwarded, with no Range or If-Match it did \
         not send; upstream: {upstream:?}"
    );

    // The forward re-caches the object, so the next GET is a verified hit.
    let again = harness
        .get(key, &signed_authorization_without_range(), &[])
        .await;
    assert_eq!(again.status, StatusCode::OK);
    assert!(
        again.body == current,
        "a repeat GET must return the current object"
    );
}

/// Production: the object grew by 99 bytes, and the GET returned the first
/// 587,623 bytes of the new version.
#[tokio::test]
async fn plain_get_after_the_object_grew_returns_the_whole_current_object() {
    plain_get_after_overwrite(Some(587_722)).await;
}

#[tokio::test]
async fn plain_get_after_the_object_shrank_returns_the_whole_current_object() {
    plain_get_after_overwrite(Some(586_662)).await;
}

#[tokio::test]
async fn plain_get_of_a_partially_cached_unchanged_object_returns_200() {
    plain_get_after_overwrite(None).await;
}

/// A range request the whole-object path forwards must be cached where S3 says its
/// bytes sit. Cached at `0..N-1`, a later read of the head returned the tail.
async fn forwarded_range_is_cached_at_its_offsets(
    range: &str,
    extra: &[(&str, &str)],
    (start, end): (u64, u64),
) {
    let harness = Harness::start(Duration::ZERO).await;
    let key = "forwarded/range.parquet";
    let data = object_bytes(256_000, 3);
    harness.mock.put(key, data.clone());

    let mut headers = vec![("range", range)];
    headers.extend_from_slice(extra);
    let response = harness
        .get(key, &signed_range_authorization(), &headers)
        .await;
    assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
    assert!(response.body == slice(&data, start, end));
    assert_eq!(
        harness.wait_for_extents(key, 1).await,
        vec![(start, end)],
        "the response must be cached where it sits in the object"
    );

    let head = harness.get_range(key, "bytes=0-99").await;
    assert!(
        head.body == slice(&data, 0, 99),
        "the head must not be served from another extent"
    );
    let inside = harness
        .get_range(key, &format!("bytes={}-{}", end - 99, end))
        .await;
    assert!(inside.body == slice(&data, end - 99, end));
}

#[tokio::test]
async fn cold_suffix_read_is_cached_at_the_offsets_s3_returned() {
    let range = format!("bytes=-{FOOTER}");
    forwarded_range_is_cached_at_its_offsets(&range, &[], (256_000 - FOOTER, 255_999)).await;
}

#[tokio::test]
async fn cold_open_ended_read_is_cached_at_the_offsets_s3_returned() {
    forwarded_range_is_cached_at_its_offsets("bytes=200000-", &[], (200_000, 255_999)).await;
}

#[tokio::test]
async fn no_cache_range_read_is_cached_at_the_offsets_s3_returned() {
    forwarded_range_is_cached_at_its_offsets(
        "bytes=100000-109999",
        &[("cache-control", "no-cache")],
        (100_000, 109_999),
    )
    .await;
}

/// An unsigned suffix read is resolved against the cached size. When a gap fetch
/// then meets a changed object, the fallback must forward the client's Range, which
/// S3 resolves against the current object, not the cache's stale resolution of it.
///
/// A fresh entry is what reaches the gap fetch: with `get_ttl: 0` the request first
/// revalidates with the client's Range unchanged, which already sees the change.
#[tokio::test]
async fn unsigned_suffix_read_after_the_object_grew_returns_the_current_tail() {
    let harness = Harness::start(Duration::from_secs(3600)).await;
    let key = "suffix/unsigned.parquet";
    let v0 = object_bytes(256_000, 5);
    harness.mock.put(key, v0.clone());
    let cached = harness.get_range(key, "bytes=223232-255999").await;
    assert!(cached.body == slice(&v0, 223_232, 255_999));
    assert_eq!(
        harness.wait_for_extents(key, 1).await,
        vec![(223_232, 255_999)]
    );

    let v1 = object_bytes(300_000, 6);
    harness.mock.put(key, v1.clone());

    let suffix = format!("bytes=-{FOOTER}");
    let response = harness
        .get(
            key,
            &signed_authorization_without_range(),
            &[("range", &suffix)],
        )
        .await;
    assert_eq!(response.status, StatusCode::PARTIAL_CONTENT);
    assert!(
        response.body == slice(&v1, 300_000 - FOOTER, 299_999),
        "the client asked for the current object's last {FOOTER} bytes; got {} bytes, \
         Content-Range {:?}; upstream: {:?}",
        response.body.len(),
        header(&response.headers, "content-range"),
        harness.mock.log()
    );
}
