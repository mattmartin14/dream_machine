#!/usr/bin/env python3
"""Local header-injecting proxy for BigLake Iceberg REST catalog.

Purpose:
  Work around DuckDB iceberg extension limitation: cannot set x-goog-user-project header.
  This proxy adds required headers and forwards to the real Google endpoint.

Usage:
  export PROJECT_ID=... (for billing header)
  export PORT=8080 (optional)
  python scripts/proxy_biglake.py

Then set USE_PROXY=true when running attach_biglake.py so it points to http://localhost:<PORT>

Security: Dev-only helper; does not implement auth beyond bearer token pass-through.
"""
from __future__ import annotations
import os
import sys
import logging
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
import requests

TARGET_BASE = "https://biglake.googleapis.com/iceberg/v1beta/restcatalog"
PORT = int(os.getenv("PORT", "8080"))
PROJECT_ID = os.getenv("PROJECT_ID") or ""

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):  # noqa: N802
        self.forward()

    def do_HEAD(self):  # noqa: N802
        self.forward(method="HEAD")

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length) if length else b""
        self.forward(method="POST", body=body)

    def log_message(self, format, *args):  # noqa: A003
        logging.info("%s - %s", self.address_string(), format % args)

    def forward(self, method="GET", body: bytes | None = None):
        # Reconstruct target URL
        upstream_path = self.path
        if upstream_path.startswith("http://") or upstream_path.startswith("https://"):
            # DuckDB may send absolute; strip host if so
            parsed = urllib.parse.urlparse(upstream_path)
            upstream_path = parsed.path
            if parsed.query:
                upstream_path += f"?{parsed.query}"
        target_url = f"{TARGET_BASE}{upstream_path}" if not upstream_path.startswith("/http") else upstream_path

        # Copy headers, drop hop-by-hop
        headers = {}
        for k, v in self.headers.items():
            if k.lower() in {"host", "connection", "proxy-connection", "keep-alive", "te", "trailers", "transfer-encoding", "upgrade"}:
                continue
            if k.lower() == "authorization":
                headers[k] = v  # pass through
        # Inject billing header if project known
        if PROJECT_ID:
            headers["x-goog-user-project"] = PROJECT_ID
        headers.setdefault("Accept", "application/json")

        try:
            resp = requests.request(method, target_url, headers=headers, data=body, stream=True, timeout=30)
        except Exception as e:  # noqa
            self.send_response(502)
            self.send_header("Content-Type", "text/plain")
            msg = f"Upstream request failed: {e}".encode()
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            return

        self.send_response(resp.status_code)
        # Forward limited headers
        for k, v in resp.headers.items():
            if k.lower() in {"content-length", "content-type"}:
                self.send_header(k, v)
        content = resp.content
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


def run():
    if not PROJECT_ID:
        logging.warning("PROJECT_ID not set; x-goog-user-project will be omitted.")
    server = HTTPServer(("0.0.0.0", PORT), ProxyHandler)
    logging.info("Proxy listening on port %d forwarding to %s", PORT, TARGET_BASE)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down proxy")

if __name__ == "__main__":
    run()
