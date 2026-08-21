import base64
import socket
import socketserver
import threading
import unittest

from scraping_functions.proxy_bridge import AuthenticatedProxyBridge, _read_headers


class _FakeUpstreamHandler(socketserver.BaseRequestHandler):
    def handle(self):
        request = _read_headers(self.request)
        self.server.request = request
        self.request.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        payload = self.request.recv(4)
        self.request.sendall(payload)


class ProxyBridgeTests(unittest.TestCase):
    def test_connect_tunnel_adds_upstream_authentication(self):
        upstream = socketserver.ThreadingTCPServer(("127.0.0.1", 0), _FakeUpstreamHandler)
        upstream.request = b""
        upstream_thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        upstream_thread.start()
        bridge = AuthenticatedProxyBridge(
            "127.0.0.1",
            upstream.server_address[1],
            "proxy-user",
            "proxy-secret",
        ).start()

        try:
            with socket.create_connection(bridge.server.server_address, timeout=5) as client:
                client.sendall(
                    b"CONNECT example.test:443 HTTP/1.1\r\n"
                    b"Host: example.test:443\r\n\r\n"
                )
                response = _read_headers(client)
                self.assertIn(b" 200 ", response.split(b"\r\n", 1)[0])
                client.sendall(b"ping")
                self.assertEqual(client.recv(4), b"ping")

            expected = base64.b64encode(b"proxy-user:proxy-secret")
            self.assertIn(b"Proxy-Authorization: Basic " + expected, upstream.request)
        finally:
            bridge.close()
            upstream.shutdown()
            upstream.server_close()
            upstream_thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
