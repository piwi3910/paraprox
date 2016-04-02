#!/usr/bin/python

import asyncio
import os
import sys
import time
from typing import Tuple
from urllib.parse import urlparse

import aiohttp
import aiohttp.hdrs
import aiohttp.server
from aiohttp.log import server_logger, access_logger
from aiohttp.multidict import CIMultiDictProxy
from aiohttp.protocol import RawRequestMessage
from aiohttp.streams import EmptyStreamReader

DEFAULT_CHUNK_SIZE = 16 * 1024
DEFAULT_PARALLELS = 10

files_to_parallel = ['.iso', '.zip', '.rpm']


def need_file_to_parallel(path: str) -> bool:
    pr = urlparse(path)  # type: ParseResult
    path, ext = os.path.splitext(pr.path)
    return ext.lower() in files_to_parallel


def get_bytes_ranges(length, parts):
    """ Get bytes ranges
    parts = 5

    [****************|****************|****************|****************|********]
     0               1<--range_size-->2                3                4        5
                                                                             ^
                                                                      last_range_size
    """
    range_size = length // parts
    last_range_size = range_size + length % parts
    last_range_idx = parts - 1
    bytes_ranges = []
    for part in range(0, last_range_idx):
        bytes_range = (part * range_size, ((part + 1) * range_size) - 1)
        bytes_ranges.append(bytes_range)
    last_range_offset = last_range_idx * range_size
    bytes_ranges.append((last_range_offset, last_range_offset + last_range_size - 1))
    return bytes_ranges


class Downloader:
    def __init__(self, path: str, bytes_range: Tuple[int, int], wfile):
        self.path = path
        self.bytes_range = bytes_range
        self.wfile = wfile

    async def download(self):
        pass


class ParallelDownloader:
    def __init__(
            self,
            writer,
            path: str,
            file_length: int,
            parallels: int = DEFAULT_PARALLELS):
        assert parallels > 1
        self.writer = writer
        self.path = path
        self.file_length = file_length
        self.parallels = parallels

    async def download(self):
        bytes_ranges = get_bytes_ranges(self.file_length, self.parallels)
        print(bytes_ranges)


class HttpRequestHandler(aiohttp.server.ServerHttpProtocol):
    def __init__(
            self, *, loop=None,
            keep_alive=75,
            keep_alive_on=True,
            timeout=0,
            logger=server_logger,
            access_log=access_logger,
            access_log_format=aiohttp.helpers.AccessLogger.LOG_FORMAT,
            debug=False,
            log=None,
            parallels: int = DEFAULT_PARALLELS,
            **kwargs):
        super().__init__(
            loop=loop,
            keep_alive=keep_alive,
            keep_alive_on=keep_alive_on,
            timeout=timeout,
            logger=logger,
            access_log=access_log,
            access_log_format=access_log_format,
            debug=debug,
            log=log,
            **kwargs)

        self._loop = loop
        self.parallels = parallels

    async def handle_request(self, message: RawRequestMessage, payload):
        self.keep_alive(True)

        req_data = payload if not isinstance(payload, EmptyStreamReader) else None

        if message.method == 'GET' and need_file_to_parallel(message.path):
            file_length, accept_ranges = await self.get_file_info(message.path)
            if accept_ranges and file_length is not None:
                # Process parallel
                # TODO: integrate aiohttp logging.
                self.log_message('PARALLEL %s %s [%s bytes]' % (message.method, message.path, file_length))
                await self.process_parallel(message.path, file_length)
                return
        # Process normally.
        self.log_message('%s %s' % (message.method, message.path))  # TODO: integrate aiohttp logging.
        await self.process_normally(message, req_data)

    async def process_normally(self, message: RawRequestMessage, req_data):
        session = aiohttp.ClientSession(headers=message.headers, loop=self._loop)
        try:
            async with session.request(message.method, message.path,
                                       data=req_data) as host_resp:  # type: aiohttp.ClientResponse
                client_res = aiohttp.Response(
                    self.writer, host_resp.status, http_version=message.version)

                # Process host response headers.
                for name, value in host_resp.headers.items():
                    if name == 'CONTENT-ENCODING':
                        continue
                    if name == 'TRANSFER-ENCODING':
                        if value.lower() == 'chunked':
                            client_res.enable_chunked_encoding()
                    client_res.add_header(name, value)

                if client_res.length is None and client_res.headers.get('TRANSFER-ENCODING') is None:
                    client_res.autochunked = lambda: False

                # Send headers to the client.
                client_res.send_headers()

                # Send a payload.
                while True:
                    chunk = await host_resp.content.read(DEFAULT_CHUNK_SIZE)
                    if not chunk:
                        break
                    client_res.write(chunk)

                if client_res.chunked or client_res.autochunked():
                    client_res.write_eof()

        except (aiohttp.ServerDisconnectedError, aiohttp.ClientResponseError):
            self.log_message("Connection error.")
        finally:
            session.close()

    async def process_parallel(self, path, file_length):
        pd = ParallelDownloader(self.writer, path, file_length, self.parallels)
        await pd.download()

    async def get_file_info(self, path) -> Tuple[int, bool]:
        """Make a HEAD request to get a 'content-length' and 'accept-ranges' headers."""
        session = aiohttp.ClientSession(loop=self._loop)
        try:
            async with session.request('HEAD', path) as res:  # type: aiohttp.ClientResponse
                hrh = res.headers  # type: CIMultiDictProxy
                accept_ranges = hrh.get('accept-ranges').lower() == 'bytes'
                file_length = hrh.get('content-length')
                if file_length is not None:
                    file_length = int(file_length)
                return file_length, accept_ranges
        except (aiohttp.ServerDisconnectedError, aiohttp.ClientResponseError):
            self.log_message("Connection error.")
        finally:
            session.close()

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
                         (self.get_client_address(),
                          self.get_log_date_time_string(),
                          mformat % args))

    def get_client_address(self):
        address, port = self.transport.get_extra_info('peername')
        return '%s:%s' % (address, port)

    def get_log_date_time_string(self):
        """Return the current time formatted for logging."""
        now = time.time()
        year, month, day, hh, mm, ss, x, y, z = time.localtime(now)
        s = "%02d/%3s/%04d %02d:%02d:%02d" % (
            day, self.monthname[month], year, hh, mm, ss)
        return s

    weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    monthname = [None,
                 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    f = loop.create_server(
        lambda: HttpRequestHandler(loop=loop, debug=True, keep_alive=75),
        '127.0.0.1', '8880')
    srv = loop.run_until_complete(f)
    print('serving on', srv.sockets[0].getsockname())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
