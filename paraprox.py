#!/usr/bin/python

import os
import sys
from http import HTTPStatus
from http.client import HTTPConnection, HTTPMessage, HTTPResponse, RemoteDisconnected
from http.server import HTTPServer, BaseHTTPRequestHandler
from socket import SocketIO, socket
from socketserver import ThreadingMixIn
from typing import Tuple, Union, Optional
from urllib.parse import urlparse, ParseResult

DEFAULT_SERVER_ADDRESS = ('127.0.0.1', 8880)
__version__ = "0.1"
_UNKNOWN = 'UNKNOWN'
_MAX_BUFFER_SIZE = 16 * 1024

files_to_parallel = ['.iso', '.zip', '.rpm']
file_chunk_length = 10 * 1024 * 1024
max_parallel_downloads = 10


class ParaproxHTTPRequestHandler(BaseHTTPRequestHandler):
    server_version = "Paraprox/" + __version__
    protocol_version = "HTTP/1.1"

    def __init__(self, request, client_address, server):
        self.host = None  # type: Optional[str]
        self.host_connection = None  # type: HTTPConnection
        self.headers = None  # type: HTTPMessage
        self.content_length = _UNKNOWN  # type: Union[int, str]
        self.host_response = None  # type: HTTPResponse
        super().__init__(request, client_address, server)

    def handle_one_request(self):
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if len(self.raw_requestline) > 65536:
                self.requestline = ''
                self.request_version = ''
                self.command = ''
                self.send_error(HTTPStatus.REQUEST_URI_TOO_LONG)
                return
            if not self.raw_requestline:
                self.close_connection = True
                return
            if not self.parse_request():
                # An error code has been sent, just exit
                return
            self.traverse_request()
            self.wfile.flush()  # actually send the response if not already done.
        except socket.timeout as e:
            # a read or a write timed out.  Discard this connection
            self.log_error("Request timed out: %r", e)
            self.close_connection = True
            return

    def is_file_to_parallel(self) -> bool:
        pr = urlparse(self.path)  # type: ParseResult
        path, ext = os.path.splitext(pr.path)
        return ext.lower() in files_to_parallel

    def get_file_info(self) -> Tuple[int, bool]:
        """Make a HEAD request to get a 'content-length' and 'accept-ranges' headers."""
        host_conn = HTTPConnection(self.host)
        try:
            host_conn.request('HEAD', self.path, headers=self.headers)
            hr = host_conn.getresponse()
            hrh = hr.headers  # type: HTTPMessage
            accept_ranges = hrh.get('accept-ranges') == 'bytes'
            file_length = hrh.get('content-length')
            if file_length is not None:
                file_length = int(file_length)
            return file_length, accept_ranges
        except (RemoteDisconnected, BrokenPipeError):
            self.log_message('Disconnected.')
        finally:
            host_conn.close()

    def parse_request(self):
        if not super().parse_request():
            return False

        self.host = self.headers.get('host')
        if not isinstance(self.host, str):
            self.send_error(HTTPStatus.BAD_REQUEST, "Request must contain a 'host' header.")
            return False

        self.content_length = self.headers.get('content-length')
        if self.content_length is not None:
            self.content_length = int(self.content_length)

        return True

    def traverse_request_parallel(self, file_length: int):
        self.log_message('Downloading file (%d bytes): `%s` in parallel...', file_length, self.path)
        self.traverse_request_normal()  # TODO: implement.

    def traverse_request_normal(self):
        body = None
        if isinstance(self.content_length, int) and self.content_length > 0:
            body = self.rfile  # We have a content to send, so set body as read file.

        host_conn = HTTPConnection(self.host)
        try:
            # Make a request to the host and get a response.
            host_conn.request(self.command, self.path, body, self.headers)
            self.host_response = host_conn.getresponse()
            self.traverse_response()
        except (RemoteDisconnected, BrokenPipeError):
            self.log_message('Disconnected.')
        finally:
            host_conn.close()

    def traverse_request(self):
        if self.command == 'GET' and self.is_file_to_parallel():
            file_length, accept_ranges = self.get_file_info()
            if accept_ranges and file_length is not None and file_length > file_chunk_length:
                self.traverse_request_parallel(file_length)
        else:
            self.traverse_request_normal()

    def traverse_response(self):
        self.traverse_response_headers()
        self.traverse_response_body()

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
    run(DEFAULT_SERVER_ADDRESS)
