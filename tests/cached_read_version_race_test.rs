//! A validated cached read must keep its object identity through the file open.

mod common;

use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{HeaderMap, Method, Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use s3_proxy::cache_types::{CacheMetadata, ObjectMetadata};
use s3_proxy::config::{CacheConfig, Config, DownloadCoordinationConfig};
use s3_proxy::connection_pool::ConnectionPoolManager;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::{Result, S3ClientApi, S3RequestContext, S3Response, S3ResponseBody};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Notify, RwLock};

use common::{StubResponse, StubS3Client};

const KEY: &str = "bucket/harness/form.parquet";
const OLD_ETAG: &str = "\"version-a\"";
const NEW_ETAG: &str = "\"version-b\"";
const RANGE: &str = "bytes=0-511";
const OBJECT_SIZE: u64 = 4096;
const LAST_MODIFIED: &str = "Tue, 13 Oct 2026 00:00:00 GMT";
const AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=TEST/20261013/ap-southeast-2/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=test";
const TIMEOUT: Duration = Duration::from_secs(10);

struct PausedRevalidation {
    stub: StubS3Client,
    pause_next: AtomicBool,
    paused: Notify,
    release: Notify,
    stream_next: AtomicBool,
    stream_release: Arc<Notify>,
}

#[async_trait]
impl S3ClientApi for PausedRevalidation {
    async fn forward_request(&self, context: S3RequestContext) -> Result<S3Response> {
        let is_old_revalidation = (context.method == Method::GET || context.method == Method::HEAD)
            && context
                .headers
                .iter()
                .any(|(key, value)| key.eq_ignore_ascii_case("if-none-match") && value == OLD_ETAG);
        let pause = is_old_revalidation && self.pause_next.swap(false, Ordering::SeqCst);
        let stream =
            context.method == Method::GET && self.stream_next.swap(false, Ordering::SeqCst);
        let mut response = self.stub.forward_request(context).await?;
        if !pause {
            if stream {
                let body = match response.body.take() {
                    Some(S3ResponseBody::Buffered(body)) => body,
                    _ => panic!("streaming fallback fixture requires a buffered source body"),
                };
                response.body = Some(S3ResponseBody::Streaming(
                    blocked_body(body, self.stream_release.clone()).await,
                ));
            }
            return Ok(response);
        }

        // S3 has validated A, but the proxy has not yet received that verdict.
        // Keep the verdict fixed while a second HTTP request publishes B.
        self.paused.notify_one();
        self.release.notified().await;
        Ok(S3Response {
            status: StatusCode::NOT_MODIFIED,
            headers: HashMap::from([
                ("etag".to_string(), OLD_ETAG.to_string()),
                ("last-modified".to_string(), LAST_MODIFIED.to_string()),
            ]),
            body: None,
            request_duration: Duration::ZERO,
        })
    }

    fn extract_metadata_from_response(&self, headers: &HashMap<String, String>) -> CacheMetadata {
        self.stub.extract_metadata_from_response(headers)
    }

    fn extract_object_metadata_from_response(
        &self,
        headers: &HashMap<String, String>,
    ) -> ObjectMetadata {
        self.stub.extract_object_metadata_from_response(headers)
    }

    fn get_connection_pool(&self) -> Arc<RwLock<ConnectionPoolManager>> {
        self.stub.get_connection_pool()
    }

    fn has_endpoint_overrides(&self) -> bool {
        self.stub.has_endpoint_overrides()
    }

    async fn set_metrics_manager(
        &self,
        metrics_manager: Arc<RwLock<s3_proxy::metrics::MetricsManager>>,
    ) {
        self.stub.set_metrics_manager(metrics_manager).await;
    }

    async fn register_endpoint(&self, endpoint: &str) {
        self.stub.register_endpoint(endpoint).await;
    }

    async fn refresh_dns(&self) -> Result<()> {
        self.stub.refresh_dns().await
    }
}

async fn blocked_body(body: Bytes, release: Arc<Notify>) -> hyper::body::Incoming {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 256];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = socket.read(&mut buffer).await.unwrap();
            assert!(read > 0, "origin connection ended before request headers");
            request.extend_from_slice(&buffer[..read]);
        }
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len(),
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        socket.write_all(&body[..1]).await.unwrap();
        release.notified().await;
        socket.write_all(&body[1..]).await.unwrap();
        socket.shutdown().await.unwrap();
    });
    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .build_http::<Full<Bytes>>();
    client
        .get(format!("http://{addr}/body").parse().unwrap())
        .await
        .unwrap()
        .into_body()
}

fn head(etag: &str) -> StubResponse {
    StubResponse::with_status(StatusCode::OK)
        .with_header("etag", etag)
        .with_header("last-modified", LAST_MODIFIED)
        .with_header("content-length", OBJECT_SIZE.to_string())
}

fn range(etag: &str, fill: u8) -> StubResponse {
    StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_header("etag", etag)
        .with_header("last-modified", LAST_MODIFIED)
        .with_header("content-range", format!("bytes 0-511/{OBJECT_SIZE}"))
        .with_body(Bytes::from(vec![fill; 512]))
}

async fn request(addr: SocketAddr, method: Method) -> (StatusCode, HeaderMap, Bytes) {
    let range = (method == Method::GET).then_some(RANGE);
    request_with_range(addr, method, range).await
}

async fn response_head(
    addr: SocketAddr,
    method: Method,
    range: Option<&str>,
) -> hyper::Response<hyper::body::Incoming> {
    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .build_http::<Full<Bytes>>();
    let mut builder = Request::builder()
        .method(method)
        .uri(format!("http://{addr}/{KEY}"))
        .header("host", "s3.ap-southeast-2.amazonaws.com")
        .header("authorization", AUTHORIZATION);
    if let Some(range) = range {
        builder = builder.header("range", range);
    }
    client
        .request(builder.body(Full::new(Bytes::new())).unwrap())
        .await
        .unwrap()
}

async fn request_with_range(
    addr: SocketAddr,
    method: Method,
    range: Option<&str>,
) -> (StatusCode, HeaderMap, Bytes) {
    tokio::time::timeout(TIMEOUT, async {
        let response = response_head(addr, method, range).await;
        let (parts, body) = response.into_parts();
        let body = body.collect().await.unwrap().to_bytes();
        (parts.status, parts.headers, body)
    })
    .await
    .expect("HTTP request did not complete")
}

async fn consolidate_range(proxy: &HttpProxy, etag: &str) {
    consolidate_extent(proxy, etag, 511).await;
}

async fn consolidate_extent(proxy: &HttpProxy, etag: &str, end: u64) {
    tokio::time::timeout(TIMEOUT, async {
        let manager = proxy.get_cache_manager();
        let consolidator = manager.get_journal_consolidator().await.unwrap();
        loop {
            consolidator.run_consolidation_cycle().await.unwrap();
            if manager
                .get_metadata_from_disk(KEY)
                .await
                .unwrap()
                .is_some_and(|metadata| {
                    metadata.object_metadata.etag == etag
                        && metadata
                            .ranges
                            .iter()
                            .any(|range| range.start == 0 && range.end == end)
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("HTTP range write did not reach consolidated metadata");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_cached_read_after_overwrite_serves_matching_version() {
    revalidation_then_overwrite(1024).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_cached_read_after_overwrite_serves_matching_version() {
    revalidation_then_overwrite(128).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn head_range_refetch_preserves_method_and_signed_headers_after_overwrite() {
    let temp = TempDir::new().unwrap();
    let (proxy, origin, addr, server) = fixture(&temp, 1024).await;
    assert_eq!(request(addr, Method::HEAD).await.0, StatusCode::OK);
    origin.stub.clone().with_default(range(OLD_ETAG, b'A'));
    assert_eq!(request(addr, Method::GET).await.2, vec![b'A'; 512]);
    consolidate_range(&proxy, OLD_ETAG).await;

    origin.pause_next.store(true, Ordering::SeqCst);
    let old_head = tokio::spawn(request_with_range(addr, Method::HEAD, Some(RANGE)));
    tokio::time::timeout(TIMEOUT, origin.paused.notified())
        .await
        .unwrap();
    origin.stub.clone().with_default(head(NEW_ETAG));
    assert_eq!(request(addr, Method::HEAD).await.0, StatusCode::OK);
    origin.stub.clone().with_default(range(NEW_ETAG, b'B'));
    assert_eq!(request(addr, Method::GET).await.2, vec![b'B'; 512]);
    consolidate_range(&proxy, NEW_ETAG).await;
    origin
        .stub
        .clone()
        .with_default(head(NEW_ETAG).with_header("content-length", "512"));

    let before_release = origin.stub.captured().len();
    origin.release.notify_one();
    let (status, headers, body) = old_head.await.unwrap();
    server.abort();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("etag").unwrap(), NEW_ETAG);
    assert_eq!(headers.get("content-length").unwrap(), "512");
    assert!(body.is_empty());
    let requests = origin.stub.captured();
    let refetches = &requests[before_release..];
    assert!(
        !refetches.is_empty(),
        "stale HEAD snapshot must refetch from S3"
    );
    assert!(refetches.iter().all(|request| {
        request.method == Method::HEAD
            && request.authorization() == Some(AUTHORIZATION)
            && request
                .headers
                .iter()
                .any(|(key, value)| key.eq_ignore_ascii_case("range") && value == RANGE)
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whole_object_refetch_preserves_fresh_body_and_checksum_headers() {
    const CHECKSUM: &str = "clvNbGbQKs9uvquckkEOAQ6iLjNodiVqrwWiEfTOGQI=";
    let temp = TempDir::new().unwrap();
    let (proxy, origin, addr, server) = fixture(&temp, 1024).await;
    assert_eq!(request(addr, Method::HEAD).await.0, StatusCode::OK);
    origin.stub.clone().with_default(
        StubResponse::ok(vec![b'A'; OBJECT_SIZE as usize])
            .with_header("etag", OLD_ETAG)
            .with_header("last-modified", LAST_MODIFIED),
    );
    assert_eq!(
        request_with_range(addr, Method::GET, None).await.2,
        vec![b'A'; OBJECT_SIZE as usize]
    );
    consolidate_extent(&proxy, OLD_ETAG, OBJECT_SIZE - 1).await;

    origin.pause_next.store(true, Ordering::SeqCst);
    let old_read = tokio::spawn(request_with_range(addr, Method::GET, None));
    tokio::time::timeout(TIMEOUT, origin.paused.notified())
        .await
        .unwrap();
    origin.stub.clone().with_default(head(NEW_ETAG));
    assert_eq!(request(addr, Method::HEAD).await.0, StatusCode::OK);
    origin.stub.clone().with_default(
        StubResponse::ok(vec![b'B'; OBJECT_SIZE as usize])
            .with_header("etag", NEW_ETAG)
            .with_header("last-modified", LAST_MODIFIED)
            .with_header("x-amz-checksum-sha256", CHECKSUM),
    );
    assert_eq!(
        request_with_range(addr, Method::GET, None).await.2,
        vec![b'B'; OBJECT_SIZE as usize]
    );
    consolidate_extent(&proxy, NEW_ETAG, OBJECT_SIZE - 1).await;

    let before_release = origin.stub.captured().len();
    origin.release.notify_one();
    let (status, headers, body) = old_read.await.unwrap();
    server.abort();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("etag").unwrap(), NEW_ETAG);
    assert_eq!(headers.get("x-amz-checksum-sha256").unwrap(), CHECKSUM);
    assert_eq!(body, vec![b'B'; OBJECT_SIZE as usize]);
    let requests = origin.stub.captured();
    let refetches = &requests[before_release..];
    assert!(!refetches.is_empty());
    assert!(refetches.iter().all(|request| {
        request.method == Method::GET
            && request.authorization() == Some(AUTHORIZATION)
            && !request
                .headers
                .keys()
                .any(|key| key.eq_ignore_ascii_case("range"))
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_range_refetch_holds_request_permit_until_stream_finishes() {
    let temp = TempDir::new().unwrap();
    let (proxy, origin, addr, server) = fixture(&temp, 1024).await;
    assert_eq!(request(addr, Method::HEAD).await.0, StatusCode::OK);
    origin.stub.clone().with_default(range(OLD_ETAG, b'A'));
    assert_eq!(request(addr, Method::GET).await.2, vec![b'A'; 512]);
    consolidate_range(&proxy, OLD_ETAG).await;

    origin.pause_next.store(true, Ordering::SeqCst);
    let old_read = tokio::spawn(response_head(addr, Method::GET, Some(RANGE)));
    tokio::time::timeout(TIMEOUT, origin.paused.notified())
        .await
        .unwrap();
    origin.stub.clone().with_default(head(NEW_ETAG));
    assert_eq!(request(addr, Method::HEAD).await.0, StatusCode::OK);
    origin.stub.clone().with_default(range(NEW_ETAG, b'B'));
    assert_eq!(request(addr, Method::GET).await.2, vec![b'B'; 512]);
    consolidate_range(&proxy, NEW_ETAG).await;

    origin.stream_next.store(true, Ordering::SeqCst);
    origin.release.notify_one();
    let response = tokio::time::timeout(TIMEOUT, old_read)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers().get("etag").unwrap(), NEW_ETAG);
    let semaphore = proxy.get_request_semaphore();
    assert_eq!(
        semaphore.available_permits(),
        3,
        "the refetched origin body still owns its request permit"
    );

    origin.stream_release.notify_one();
    let body = tokio::time::timeout(TIMEOUT, response.into_body().collect())
        .await
        .unwrap()
        .unwrap()
        .to_bytes();
    assert_eq!(body, vec![b'B'; 512]);
    tokio::time::timeout(TIMEOUT, async {
        while semaphore.available_permits() != 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completed refetch retained its request permit");
    server.abort();
}

async fn fixture(
    temp: &TempDir,
    disk_streaming_threshold: u64,
) -> (
    Arc<HttpProxy>,
    Arc<PausedRevalidation>,
    SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let mut config = Config {
        cache: CacheConfig {
            cache_dir: temp.path().to_path_buf(),
            ram_cache_enabled: false,
            write_cache_enabled: false,
            get_ttl: Duration::ZERO,
            head_ttl: Duration::ZERO,
            disk_streaming_threshold,
            actively_remove_cached_data: false,
            download_coordination: DownloadCoordinationConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    config.server.max_concurrent_requests = 4;
    let config = Arc::new(config);
    let proxy = Arc::new(HttpProxy::new("127.0.0.1:0".parse().unwrap(), config.clone()).unwrap());
    proxy.get_cache_manager().initialize().await.unwrap();
    proxy
        .get_range_handler()
        .get_disk_cache_manager()
        .write()
        .await
        .initialize()
        .await
        .unwrap();
    let origin = Arc::new(PausedRevalidation {
        stub: StubS3Client::new().with_default(head(OLD_ETAG)),
        pause_next: AtomicBool::new(false),
        paused: Notify::new(),
        release: Notify::new(),
        stream_next: AtomicBool::new(false),
        stream_release: Arc::new(Notify::new()),
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_proxy = proxy.clone();
    let server_origin = origin.clone();
    let server = tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let proxy = server_proxy.clone();
            let s3_client: Arc<dyn S3ClientApi + Send + Sync> = server_origin.clone();
            let config = config.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req| {
                    HttpProxy::handle_request(
                        req,
                        peer,
                        config.clone(),
                        proxy.get_cache_manager(),
                        s3_client.clone(),
                        proxy.get_range_handler(),
                        proxy.get_request_semaphore(),
                        None,
                        None,
                        proxy.get_inflight_tracker(),
                        None,
                        None,
                        None,
                        s3_client.get_inflight_ledger(),
                    )
                });
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                    .unwrap();
            });
        }
    });

    (proxy, origin, addr, server)
}

async fn revalidation_then_overwrite(disk_streaming_threshold: u64) {
    let temp = TempDir::new().unwrap();
    let (proxy, origin, addr, server) = fixture(&temp, disk_streaming_threshold).await;

    assert_eq!(request(addr, Method::HEAD).await.0, StatusCode::OK);
    origin.stub.clone().with_default(range(OLD_ETAG, b'A'));
    assert_eq!(request(addr, Method::GET).await.2, vec![b'A'; 512]);
    consolidate_range(&proxy, OLD_ETAG).await;

    origin.pause_next.store(true, Ordering::SeqCst);
    let old_read = tokio::spawn(request(addr, Method::GET));
    tokio::time::timeout(TIMEOUT, origin.paused.notified())
        .await
        .expect("cached GET did not revalidate version A");

    origin.stub.clone().with_default(head(NEW_ETAG));
    let (status, headers, _) = request(addr, Method::HEAD).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("etag").unwrap(), NEW_ETAG);
    origin.stub.clone().with_default(range(NEW_ETAG, b'B'));
    assert_eq!(request(addr, Method::GET).await.2, vec![b'B'; 512]);
    consolidate_range(&proxy, NEW_ETAG).await;

    // A's old verdict must not authenticate the B file now at the same offsets.
    origin.release.notify_one();
    let (status, headers, body) = old_read.await.unwrap();
    server.abort();
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(headers.get("etag").unwrap(), NEW_ETAG);
    assert_eq!(body, vec![b'B'; 512]);

    let captured = origin.stub.captured();
    assert!(captured
        .iter()
        .all(|request| request.authorization() == Some(AUTHORIZATION)));
    assert!(captured.iter().any(|request| {
        request.method == Method::GET && request.if_none_match() == Some(OLD_ETAG)
    }));
    assert!(
        captured
            .iter()
            .filter(|request| request.method == Method::GET)
            .count()
            >= 4,
        "the old cached read must fetch the current version after its snapshot changes"
    );
}
