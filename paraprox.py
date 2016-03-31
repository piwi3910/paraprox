#!/usr/bin/python

import sys
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
        self.traverse_response()

    def do_POST(self):
        self.traverse_response()

    def traverse_response(self):
        self.host = self.headers.get('host')
        assert isinstance(self.host, str)

        body = None
        self.content_length = self.headers.get('content-length')
        if self.content_length is not None:
            self.content_length = int(self.content_length)
            if self.content_length > 0:
                body = self.rfile  # We have a content to send, so set body as read file.

        host_conn = HTTPConnection(self.host)

        try:
            # Make a request to the host and get a response.
            host_conn.request(self.command, self.path, body, self.headers)
            self.host_response = host_conn.getresponse()

            self.traverse_response_headers()
            self.traverse_response_body()
        except BrokenPipeError:
            self.log_message('Disconnected unexpectedly.')
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

    def log_message(self, mformat, *args):
        """Log an arbitrary message.

        This is used by all other logging functions.  Override
        it if you have specific logging wishes.

        The first argument, FORMAT, is a format string for the
        message to be logged.  If the format string contains
        any % escapes requiring parameters, they should be
        specified as subsequent arguments (it's just like
        printf!).

        The client ip and current date/time are prefixed to
        every message.

        """
        sys.stderr.write("%s - - [%s] %s\n" %
                         (self.address_string(),
                          self.log_date_time_string(),
                          mformat % args))

    def address_string(self):
        """Return the client address."""
        return '%s:%s' % (self.client_address[0], self.client_address[1])


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
