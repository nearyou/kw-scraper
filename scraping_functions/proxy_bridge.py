"""Local forwarding proxy that gives Chromium authenticated upstream access."""

import base64
import select
import socket
import socketserver
import ssl
import threading


MAX_HEADER_BYTES = 64 * 1024


def _read_headers(connection):
    data = bytearray()
    while b"\r\n\r\n" not in data:
        chunk = connection.recv(4096)
        if not chunk:
            break
        data.extend(chunk)
        if len(data) > MAX_HEADER_BYTES:
            raise OSError("Proxy headers exceed the safety limit")
    return bytes(data)


def _add_proxy_authorization(request, authorization):
    header, separator, body = request.partition(b"\r\n\r\n")
    lines = header.split(b"\r\n")
    filtered = [
        line
        for line in lines
        if not line.lower().startswith(b"proxy-authorization:")
    ]
    filtered.append(b"Proxy-Authorization: Basic " + authorization)
    return b"\r\n".join(filtered) + separator + body


def _relay(left, right):
    sockets = (left, right)
    while True:
        readable, _, _ = select.select(sockets, (), (), 30)
        if not readable:
            continue
        for source in readable:
            data = source.recv(64 * 1024)
            if not data:
                return
            destination = right if source is left else left
            destination.sendall(data)


class _BridgeServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class _BridgeHandler(socketserver.BaseRequestHandler):
    def handle(self):
        upstream = None
        try:
            request = _read_headers(self.request)
            if not request:
                return

            upstream = socket.create_connection(
                (self.server.upstream_host, self.server.upstream_port),
                timeout=self.server.connect_timeout,
            )
            if self.server.upstream_tls:
                context = ssl.create_default_context()
                upstream = context.wrap_socket(
                    upstream, server_hostname=self.server.upstream_host
                )

            upstream.sendall(
                _add_proxy_authorization(request, self.server.authorization)
            )

            method = request.split(b" ", 1)[0].upper()
            if method == b"CONNECT":
                response = _read_headers(upstream)
                self.request.sendall(response)
                status_line = response.split(b"\r\n", 1)[0]
                if b" 200 " not in status_line:
                    return

            _relay(self.request, upstream)
        except (OSError, ssl.SSLError):
            # Chromium receives a closed proxy connection and reports the
            # navigation failure through the normal scraper error path.
            return
        finally:
            if upstream is not None:
                try:
                    upstream.close()
                except OSError:
                    pass


class AuthenticatedProxyBridge:
    """Forward localhost Chromium traffic through an authenticated HTTP proxy."""

    def __init__(
        self,
        host,
        port,
        username,
        password,
        *,
        scheme="http",
        connect_timeout=30,
    ):
        self.host = str(host)
        self.port = int(port)
        self.username = str(username)
        self.password = str(password)
        self.scheme = str(scheme).lower()
        self.connect_timeout = float(connect_timeout)
        self.server = None
        self.thread = None

    def start(self):
        if self.server is not None:
            return self
        if self.scheme not in {"http", "https"}:
            raise ValueError("Authenticated proxy bridge requires HTTP or HTTPS")

        server = _BridgeServer(("127.0.0.1", 0), _BridgeHandler)
        server.upstream_host = self.host
        server.upstream_port = self.port
        server.upstream_tls = self.scheme == "https"
        server.connect_timeout = self.connect_timeout
        credentials = f"{self.username}:{self.password}".encode("utf-8")
        server.authorization = base64.b64encode(credentials)
        thread = threading.Thread(
            target=server.serve_forever,
            name="authenticated-proxy-bridge",
            daemon=True,
        )
        thread.start()
        self.server = server
        self.thread = thread
        return self

    @property
    def url(self):
        if self.server is None:
            raise RuntimeError("Proxy bridge has not been started")
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def close(self):
        server, self.server = self.server, None
        thread, self.thread = self.thread, None
        if server is None:
            return
        server.shutdown()
        server.server_close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)
