"""Exercise the proxy binary with real PyArrow S3 dataset range reads.

Run: uv run --with pyarrow tests/parquet_overwrite_e2e.py target/release/s3-proxy
Add --expect-corruption to verify the failure against a pre-fix binary.
"""

import argparse
import hashlib
import http.client
import json
import socket
import statistics
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from pyarrow import fs


def parquet(version: int) -> bytes:
    table = pa.table(
        {
            "padding": [("A" * 16 if version == 0 else "B" * 30)] * 10000,
            "runner_id": list(range(version * 10000, (version + 1) * 10000)),
            "source": ["RISE"] * 10000,
        }
    )
    sink = pa.BufferOutputStream()
    pq.write_table(
        table, sink, compression="NONE", use_dictionary=False, row_group_size=2000
    )
    return sink.getvalue().to_pybytes()


def run(
    binary: str,
    expect_corruption: bool,
    benchmark_reads: int,
    benchmark_operations: int,
) -> None:
    bodies = [parquet(0), parquet(1)]
    current = [0]

    class S3(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self):
            self.respond(False)

        def do_GET(self):
            self.respond(True)

        def log_message(self, *_args):
            pass

        def respond(self, send_body: bool):
            body = bodies[current[0]]
            etag = '"' + hashlib.md5(body).hexdigest() + '"'
            status = 200
            start, end = 0, len(body) - 1
            requested = self.headers.get("Range")
            if self.headers.get("If-None-Match") == etag:
                status = 304
            elif self.headers.get("If-Match") not in (None, etag):
                status = 412
            elif requested:
                lo, hi = requested.removeprefix("bytes=").split("-")
                start = int(lo) if lo else max(0, len(body) - int(hi))
                end = min(int(hi), len(body) - 1) if lo and hi else len(body) - 1
                status = 206
            payload = body[start : end + 1] if status in (200, 206) else b""
            self.send_response(status)
            self.send_header("ETag", etag)
            self.send_header("Last-Modified", "Tue, 13 Oct 2026 00:00:00 GMT")
            self.send_header("Content-Length", str(len(payload)))
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
            self.end_headers()
            if send_body:
                self.wfile.write(payload)

    upstream = ThreadingHTTPServer(("127.0.0.1", 0), S3)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    endpoint = f"127.0.0.1:{upstream.server_port}"
    with socket.socket() as free_port:
        free_port.bind(("127.0.0.1", 0))
        port = free_port.getsockname()[1]

    with tempfile.TemporaryDirectory(prefix="s3-parquet-e2e-") as temp:
        root = Path(temp)
        cache = root / "cache"
        config = {
            "server": {"mode": "proxy_only", "proxy_port": port},
            "cache": {
                "cache_dir": str(cache),
                "ram_cache_enabled": False,
                "write_cache_enabled": False,
                "get_ttl": "0s",
                "head_ttl": "0s",
                "actively_remove_cached_data": False,
                "shared_storage": {"consolidation_interval": "60s"},
            },
            "connection_pool": {"upstream_overrides": {endpoint: {"scheme": "http"}}},
            "health": {"enabled": False},
            "metrics": {"enabled": False},
            "dashboard": {"enabled": False},
            "logging": {
                "app_log_dir": str(root / "logs"),
                "access_log_dir": str(root / "access"),
            },
        }
        path = root / "config.yaml"
        path.write_text(json.dumps(config))
        with (root / "proxy.log").open("w") as log:
            process = subprocess.Popen(
                [binary, "--config", str(path)], stdout=log, stderr=log
            )
            try:
                deadline = time.monotonic() + 10
                while True:
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                            break
                    except OSError:
                        if process.poll() is not None or time.monotonic() >= deadline:
                            raise RuntimeError((root / "proxy.log").read_text())
                        time.sleep(0.05)
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
                url = f"http://{endpoint}/bucket/harness/form.parquet"
                headers = {
                    "Host": endpoint,
                    "Authorization": "AWS4-HMAC-SHA256 Credential=TEST/20261013/ap-southeast-2/s3/aws4_request, SignedHeaders=host;range;x-amz-date, Signature=test",
                }
                conn.request("HEAD", url, headers=headers)
                response = conn.getresponse()
                assert response.status == 200
                response.read()
                conn.request(
                    "GET",
                    url,
                    headers={**headers, "Range": f"bytes=0-{len(bodies[0]) - 65537}"},
                )
                response = conn.getresponse()
                assert response.status == 206
                response.read()
                deadline = time.monotonic() + 10
                while not any(
                    p.stat().st_size
                    for p in (cache / "metadata" / "_journals").glob("*.journal")
                ):
                    if time.monotonic() >= deadline:
                        raise AssertionError("old range was not journaled")
                    time.sleep(0.01)
                current[0] = 0 if benchmark_reads or benchmark_operations else 1
                conn.request("HEAD", url, headers=headers)
                response = conn.getresponse()
                assert response.status == 200
                response.read()
                conn.close()
                filesystem = fs.S3FileSystem(
                    access_key="TEST",
                    secret_key="TEST",
                    region="ap-southeast-2",
                    scheme="http",
                    endpoint_override=endpoint,
                    proxy_options={"scheme": "http", "host": "127.0.0.1", "port": port},
                    force_virtual_addressing=False,
                )
                dataset = ds.dataset(
                    "bucket/harness/form.parquet",
                    filesystem=filesystem,
                    format="parquet",
                )
                try:
                    table = dataset.to_table(
                        columns=["runner_id"], filter=ds.field("source") == "RISE"
                    )
                except OSError as error:
                    if not expect_corruption:
                        raise
                    assert "Deserializing page header failed" in str(error), str(error)
                    print("REPRODUCED:", error)
                else:
                    assert not expect_corruption, (
                        "baseline unexpectedly read a coherent Parquet file"
                    )
                    first_id = current[0] * 10000
                    assert table["runner_id"].to_pylist() == list(
                        range(first_id, first_id + 10000)
                    )
                    print("PASS: all 10000 filtered rows came from the current version")
                    if benchmark_reads:
                        durations = []
                        for _ in range(benchmark_reads):
                            start = time.perf_counter()
                            result = dataset.to_table(
                                columns=["runner_id"],
                                filter=ds.field("source") == "RISE",
                            )
                            durations.append((time.perf_counter() - start) * 1000)
                            assert result.equals(table)
                        durations.sort()
                        print(
                            f"BENCHMARK: n={benchmark_reads}, median={statistics.median(durations):.2f}ms, p95={durations[int(0.95 * (len(durations) - 1))]:.2f}ms"
                        )
                    if benchmark_operations:
                        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
                        for name, method, requested_range in [
                            ("HEAD", "HEAD", None),
                            ("full GET", "GET", None),
                            ("range GET", "GET", "bytes=0-511"),
                        ]:
                            op_headers = dict(headers)
                            if requested_range:
                                op_headers["Range"] = requested_range
                            else:
                                op_headers["Authorization"] = headers[
                                    "Authorization"
                                ].replace("host;range;", "host;")
                            durations = []
                            for i in range(benchmark_operations + 2):
                                start = time.perf_counter()
                                operation_url = (
                                    url.replace("form.parquet", "whole.parquet")
                                    if name == "full GET"
                                    else url
                                )
                                conn.request(method, operation_url, headers=op_headers)
                                response = conn.getresponse()
                                body = response.read()
                                elapsed = (time.perf_counter() - start) * 1000
                                assert response.status == (
                                    206 if requested_range else 200
                                ), response.status
                                assert body == (
                                    b""
                                    if method == "HEAD"
                                    else bodies[0][:512]
                                    if requested_range
                                    else bodies[0]
                                )
                                if i >= 2:
                                    durations.append(elapsed)
                            durations.sort()
                            print(
                                f"{name}: n={benchmark_operations}, median={statistics.median(durations):.2f}ms, p95={durations[int(0.95 * (len(durations) - 1))]:.2f}ms"
                            )
                        conn.close()
            finally:
                process.terminate()
                process.wait(timeout=10)
                upstream.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("binary")
    parser.add_argument("--expect-corruption", action="store_true")
    parser.add_argument("--benchmark-reads", type=int, default=0)
    parser.add_argument("--benchmark-operations", type=int, default=0)
    args = parser.parse_args()
    run(
        args.binary,
        args.expect_corruption,
        args.benchmark_reads,
        args.benchmark_operations,
    )
