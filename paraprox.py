#!/usr/bin/python

from socket import SocketIO
from socketserver import ThreadingMixIn
from http.server import HTTPServer, BaseHTTPRequestHandler
from http.client import HTTPConnection, HTTPMessage, HTTPResponse
from typing import Tuple

__version__ = "0.1"

_UNKNOWN = 'UNKNOWN'

_MAX_BUFFER_SIZE = 16 * 1024


class ParaproxHTTPRequestHandler(BaseHTTPRequestHandler):
    server_version = "Paraprox/" + __version__
    protocol_version = "HTTP/1.1"

    def __init__(self, request, client_address, server):
        self.host = _UNKNOWN  # type: str
        self.host_connection = None  # type: HTTPConnection
        self.headers = None  # type: HTTPMessage
        self.content_length = _UNKNOWN  # type: int
        self.host_response = None  # type: HTTPResponse
        super().__init__(request, client_address, server)

    def do_GET(self):
        self.host = self.headers.get('host')
        assert isinstance(self.host, str)
        self.content_length = self.headers.get('content-length')
        self.traverse_response()

    def traverse_response(self):
        body = self.rfile if self.content_length else None

        host_conn = HTTPConnection(self.host)

        try:
            # Make a request to the host and get a response.
            host_conn.request(self.command, self.path, body, self.headers)
            self.host_response = host_conn.getresponse()

            self.traverse_response_headers()
            self.traverse_response_body()
        except BrokenPipeError:
            self.log_error('Connection [%s:%s] lost.' % self.client_address)
            return

    def traverse_response_headers(self):
        hr = self.host_response
        hrh = hr.headers  # type: HTTPMessage

        self.log_request(hr.code)
        self.send_response_only(hr.code)
        for key, value in hrh.items():
            self.send_header(key, value)
        self.end_headers()

    def traverse_response_body(self):
        hr = self.host_response
        s = self.wfile  # type: SocketIO
        b = bytearray(_MAX_BUFFER_SIZE)
        if hr.chunked:
            while True:
                chunk = []
                n = hr.readinto(b)
                if not n:
                    s.write(b'0\r\n\r\n')
                    break
                v = b
                if n < _MAX_BUFFER_SIZE:
                    v = memoryview(b)[:n].tobytes()
                chunk.append(str.encode('%x\r\n' % n, 'ascii'))
                chunk.append(v)
                chunk.append(b'\r\n')
                s.write(b''.join(chunk))
        else:
            while True:
                n = hr.readinto(b)
                if not n:
                    break
                v = b
                if n < _MAX_BUFFER_SIZE:
                    v = memoryview(v)[:n].tobytes()
                s.write(v)


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    pass


def run(server_address: Tuple[str, int]):
    httpd = ThreadingHTTPServer(server_address, ParaproxHTTPRequestHandler)
    try:
        print('Server started at %s.' % repr(server_address))
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print('Server stopped.')


if __name__ == '__main__':
    run(('127.0.0.1', 8880))
