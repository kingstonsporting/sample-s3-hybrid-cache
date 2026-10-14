//! Delayed range journals must not resurrect bytes from an overwritten object.

mod common;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;

use common::{StubResponse, StubS3Client};

const KEY: &str = "bucket/harness/form.parquet";
const OLD_ETAG: &str = "\"version-a\"";
const NEW_ETAG: &str = "\"version-b\"";
const RANGE: &str = "bytes=0-511";
const OBJECT_SIZE: u64 = 4096;

fn head(etag: &str) -> StubResponse {
    StubResponse::with_status(StatusCode::OK)
        .with_header("etag", etag)
        .with_header("last-modified", "Tue, 13 Oct 2026 00:00:00 GMT")
        .with_header("content-length", OBJECT_SIZE.to_string())
}

fn range(etag: &str, fill: u8) -> StubResponse {
    StubResponse::with_status(StatusCode::PARTIAL_CONTENT)
        .with_header("etag", etag)
        .with_header("last-modified", "Tue, 13 Oct 2026 00:00:00 GMT")
        .with_header("content-range", format!("bytes 0-511/{OBJECT_SIZE}"))
        .with_body(Bytes::from(vec![fill; 512]))
}

async fn request(addr: SocketAddr, method: Method, range: Option<&str>) -> (StatusCode, Bytes) {
    let client = hyper_util::client::legacy::Client::builder(TokioExecutor::new())
        .build_http::<Full<Bytes>>();
    let mut builder = Request::builder()
        .method(method)
        .uri(format!("http://{addr}/{KEY}"))
        .header("host", "s3.ap-southeast-2.amazonaws.com")
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=TEST/20261013/ap-southeast-2/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=test",
        );
    if let Some(range) = range {
        builder = builder.header("range", range);
    }
    let response = client
        .request(builder.body(Full::new(Bytes::new())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_old_range_after_new_head_never_serves_old_bytes_under_new_etag() {
    overwrite_then_read(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_old_range_after_new_head_never_serves_old_bytes_under_new_etag() {
    overwrite_then_read(false).await;
}

#[tokio::test]
async fn long_keys_publish_buffered_and_incremental_versioned_ranges() {
    let temp = TempDir::new().unwrap();
    let proxy = cache_fixture(&temp).await;
    let handler = proxy.get_range_handler();
    for length in [150, 190, 220] {
        let key = format!("bucket/{}", "x".repeat(length));
        let object = s3_proxy::cache_types::ObjectMetadata::new(
            OLD_ETAG.to_string(),
            String::new(),
            OBJECT_SIZE,
            None,
        );
        handler
            .store_range_new_storage(
                &key,
                0,
                511,
                &[b'A'; 512],
                object.clone(),
                Duration::ZERO,
                false,
            )
            .await
            .unwrap();
        let disk = handler.get_disk_cache_manager().read().await;
        let mut writer = disk
            .begin_incremental_range_write(&key, 512, 1023, false)
            .await
            .unwrap();
        s3_proxy::disk_cache::DiskCacheManager::write_range_chunk(&mut writer, &[b'A'; 512])
            .unwrap();
        disk.commit_incremental_range(writer, object, Duration::ZERO)
            .await
            .unwrap();
        let ranges = disk
            .find_pending_journal_ranges(&key, 0, 1023, Some(OLD_ETAG))
            .await
            .unwrap();
        assert_eq!(ranges.len(), 2);
        for range in ranges {
            assert!(
                std::path::Path::new(&range.file_path)
                    .file_name()
                    .unwrap()
                    .len()
                    <= 255
            );
            assert_eq!(
                disk.load_range_data_for_version(&range, &key, OLD_ETAG)
                    .await
                    .unwrap(),
                vec![b'A'; 512]
            );
            assert!(matches!(
                disk.load_range_data_for_version(&range, &key, NEW_ETAG)
                    .await,
                Err(s3_proxy::ProxyError::CacheVersionMismatch { .. })
            ));
        }
    }
}

async fn cache_fixture(temp: &TempDir) -> Arc<HttpProxy> {
    let config = Arc::new(Config {
        cache: s3_proxy::config::CacheConfig {
            cache_dir: temp.path().to_path_buf(),
            ram_cache_enabled: false,
            write_cache_enabled: false,
            get_ttl: Duration::ZERO,
            head_ttl: Duration::ZERO,
            actively_remove_cached_data: false,
            ..Default::default()
        },
        ..Default::default()
    });
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
    proxy
}

async fn overwrite_then_read(consolidate_before_read: bool) {
    let temp = TempDir::new().unwrap();
    let proxy = cache_fixture(&temp).await;
    let manager = proxy.get_cache_manager();
    let config = proxy.get_config();
    let stub = StubS3Client::new().with_default(head(OLD_ETAG));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_proxy = proxy.clone();
    let server_stub = stub.clone();
    let server = tokio::spawn(async move {
        loop {
            let (stream, peer) = listener.accept().await.unwrap();
            let proxy = server_proxy.clone();
            let s3_client = server_stub.clone().into_trait_object();
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

    assert_eq!(request(addr, Method::HEAD, None).await.0, StatusCode::OK);
    stub.clone().with_default(range(OLD_ETAG, b'A'));
    let (status, old_body) = request(addr, Method::GET, Some(RANGE)).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(old_body, vec![b'A'; 512]);

    // Receiving a buffered response does not await its background cache commit.
    // Publish A's journal before HEAD B so this exercises replay of old bytes,
    // rather than the separate fence that rejects a late A cache write.
    let handler = proxy.get_range_handler();
    let old_range = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let disk = handler.get_disk_cache_manager().read().await;
            let pending = disk
                .find_pending_journal_ranges(KEY, 0, 511, Some(OLD_ETAG))
                .await
                .unwrap();
            if let Some(range) = pending
                .into_iter()
                .find(|range| range.start == 0 && range.end == 511)
            {
                assert_eq!(disk.load_range_data(&range).await.unwrap(), vec![b'A'; 512]);
                break range;
            }
            drop(disk);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("version A cache commit did not publish its range journal");
    let old_path = temp.path().join("ranges").join(&old_range.file_path);

    // The range is journaled, but has not yet reached the object metadata.
    let before = manager.get_metadata_from_disk(KEY).await.unwrap().unwrap();
    assert!(before.ranges.is_empty());
    stub.clone().with_default(head(NEW_ETAG));
    assert_eq!(request(addr, Method::HEAD, None).await.0, StatusCode::OK);
    assert_eq!(
        manager
            .get_metadata_from_disk(KEY)
            .await
            .unwrap()
            .unwrap()
            .object_metadata
            .etag,
        NEW_ETAG
    );

    // Advance the real consolidator explicitly instead of racing its timer.
    if consolidate_before_read {
        let consolidator = manager.get_journal_consolidator().await.unwrap();
        let result = consolidator.consolidate_object(KEY).await.unwrap();
        assert!(result.success);
        assert!(result.entries_processed > 0);
        assert_eq!(result.entries_consolidated, 0);
        assert!(manager
            .get_metadata_from_disk(KEY)
            .await
            .unwrap()
            .unwrap()
            .ranges
            .is_empty());
        assert!(
            !old_path.exists(),
            "rejected A journal bytes must be removed"
        );
    } else {
        assert!(
            old_path.exists(),
            "the pending A journal must still own its bytes"
        );
    }
    stub.clone()
        .with_default(range(NEW_ETAG, b'B'))
        .with_response_for_etag(
            NEW_ETAG,
            StubResponse::not_modified_with_last_modified("Tue, 13 Oct 2026 00:00:00 GMT")
                .with_header("etag", NEW_ETAG),
        );
    let (status, body) = request(addr, Method::GET, Some(RANGE)).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        body,
        vec![b'B'; 512],
        "old journal bytes were served as the new object version"
    );

    let consolidator = manager.get_journal_consolidator().await.unwrap();
    consolidator.run_consolidation_cycle().await.unwrap();
    let old_metadata = s3_proxy::cache_types::ObjectMetadata::new(
        OLD_ETAG.to_string(),
        "Tue, 13 Oct 2026 00:00:00 GMT".to_string(),
        OBJECT_SIZE,
        None,
    );

    // A download started before the overwrite may finish after B is cached.
    let error = handler
        .store_range_new_storage(
            KEY,
            0,
            511,
            &vec![b'A'; 512],
            old_metadata.clone(),
            Duration::ZERO,
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        s3_proxy::ProxyError::CacheVersionMismatch { .. }
    ));
    let disk = handler.get_disk_cache_manager().read().await;
    let mut writer = disk
        .begin_incremental_range_write(KEY, 0, 511, false)
        .await
        .unwrap();
    s3_proxy::disk_cache::DiskCacheManager::write_range_chunk(&mut writer, &vec![b'A'; 512])
        .unwrap();
    let error = disk
        .commit_incremental_range(writer, old_metadata, Duration::ZERO)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        s3_proxy::ProxyError::CacheVersionMismatch { .. }
    ));
    drop(disk);
    assert_eq!(
        request(addr, Method::GET, Some(RANGE)).await.1,
        vec![b'B'; 512]
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_journal_keeps_one_version_and_removes_rejected_bytes() {
    let temp = TempDir::new().unwrap();
    let proxy = cache_fixture(&temp).await;
    let manager = proxy.get_cache_manager();
    let handler = proxy.get_range_handler();
    for (start, etag, fill, ttl) in [
        (0, OLD_ETAG, b'A', Duration::from_secs(3600)),
        (512, NEW_ETAG, b'B', Duration::ZERO),
    ] {
        handler
            .store_range_new_storage(
                KEY,
                start,
                start + 511,
                &[fill; 512],
                s3_proxy::cache_types::ObjectMetadata::new(
                    etag.to_string(),
                    "Tue, 13 Oct 2026 00:00:00 GMT".to_string(),
                    OBJECT_SIZE,
                    None,
                ),
                ttl,
                false,
            )
            .await
            .unwrap();
        if start == 0 {
            // Metadata can be evicted while the range journal is still pending.
            let disk = handler.get_disk_cache_manager().read().await;
            std::fs::remove_file(disk.get_new_metadata_file_path(KEY)).unwrap();
        }
    }
    let disk = handler.get_disk_cache_manager().read().await;
    assert!(disk
        .find_pending_journal_ranges(KEY, 0, 1023, None)
        .await
        .unwrap()
        .is_empty());
    let pending = disk
        .find_pending_journal_ranges(KEY, 0, 1023, Some(NEW_ETAG))
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].start, 512);
    let old_path = disk.get_versioned_range_file_path(KEY, 0, 511, OLD_ETAG);
    assert!(old_path.exists());
    let rejected_size = old_path.metadata().unwrap().len();
    drop(disk);
    let consolidator = manager.get_journal_consolidator().await.unwrap();
    consolidator.run_consolidation_cycle().await.unwrap();
    let metadata = manager.get_metadata_from_disk(KEY).await.unwrap().unwrap();
    assert_eq!(metadata.object_metadata.etag, NEW_ETAG);
    assert_eq!(metadata.ranges.len(), 1);
    assert_eq!(metadata.ranges[0].start, 512);
    assert!(metadata.expires_at <= std::time::SystemTime::now());
    assert!(
        !old_path.exists(),
        "rejected bytes must not survive for orphan recovery"
    );
    assert_eq!(
        consolidator.size_accumulator().current_delta(),
        -(rejected_size as i64)
    );
    assert_eq!(
        consolidator.size_accumulator().current_write_cache_delta(),
        0
    );

    // A pending lookup uses the caller's metadata snapshot, rather than a
    // newer disk ETag that would label B's bytes as A.
    handler
        .store_range_new_storage(
            KEY,
            1024,
            1535,
            &[b'B'; 512],
            metadata.object_metadata.clone(),
            Duration::ZERO,
            false,
        )
        .await
        .unwrap();
    let old_snapshot = s3_proxy::cache_types::NewCacheMetadata {
        object_metadata: s3_proxy::cache_types::ObjectMetadata::new(
            OLD_ETAG.to_string(),
            String::new(),
            OBJECT_SIZE,
            None,
        ),
        cache_key: KEY.to_string(),
        ..Default::default()
    };
    let overlap = handler
        .find_cached_ranges(
            KEY,
            &s3_proxy::range_handler::RangeSpec {
                start: 1024,
                end: 1535,
            },
            Some(OLD_ETAG),
            Some(&old_snapshot),
            s3_proxy::cache_types::RangeLookupPurpose::RevalidationCandidate,
        )
        .await
        .unwrap();
    assert!(overlap.cached_ranges.is_empty());

    // Versionless recovery cannot attach arbitrary orphan bytes to B.
    let writer = manager.get_hybrid_metadata_writer().await.unwrap();
    let error = writer
        .lock()
        .await
        .write_range_metadata(
            KEY,
            metadata.ranges[0].clone(),
            s3_proxy::hybrid_metadata_writer::WriteMode::Hybrid,
            None,
            Duration::ZERO,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        s3_proxy::ProxyError::CacheVersionMismatch { .. }
    ));
}
