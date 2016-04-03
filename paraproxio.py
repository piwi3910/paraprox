#!/usr/bin/python

import asyncio
import os
import shutil
import sys
import time
from asyncio import AbstractEventLoop
from typing import Tuple, Callable
from urllib.parse import urlparse

import aiohttp
import aiohttp.hdrs
import aiohttp.server
from aiohttp.log import server_logger, access_logger
from aiohttp.multidict import CIMultiDictProxy
from aiohttp.protocol import RawRequestMessage
from aiohttp.streams import EmptyStreamReader

DEFAULT_CHUNK_SIZE = 64 * 1024
DEFAULT_PARALLELS = 10

DEFAULT_BUFFER_DIR = '.paraprox_buffer'

files_to_parallel = ['.iso', '.zip', '.rpm', '.gz']


def need_file_to_parallel(path: str) -> bool:
    pr = urlparse(path)  # type: ParseResult
    path, ext = os.path.splitext(pr.path)
    return ext.lower() in files_to_parallel


def get_bytes_ranges(length, parts):
    """ Get bytes ranges """
    ###################################################################################################
    #
    # length            = 89
    # parts             = 4
    # range_size        = length // parts = 89 // 4 = 22
    # last_range_size   = range_size + length % parts = 22 + 89 % 4 = 22 + 1 = 23
    #
    # [<-----range_size----->|<-----range_size----->|<-----range_size----->|<---last_range_size---->|
    # [**********************|**********************|**********************|**********************|*]
    # 0                      22                     44                     66                    88 89
    #
    ###################################################################################################
    #
    # length            = 89
    # parts             = 5
    # range_size        = length // parts = 89 // 5 = 17
    # last_range_size   = range_size + length % parts = 17 + 89 % 5 = 17 + 4 = 21
    #
    # [<--range_size--->|<--range_size--->|<--range_size--->|<--range_size--->|<--last_range_size--->|
    # [*****************|*****************|*****************|*****************|*****************|****]
    # 0                 17                34                51                68                85   89
    #
    ###################################################################################################

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


class PartDownloadError(Exception):
    pass


class Downloader:
    def __init__(self, path: str, bytes_range: Tuple[int, int], buffer_file_path, loop: AbstractEventLoop = None):
        self._path = path
        self._bytes_range = bytes_range
        self.buffer_file_path = buffer_file_path
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._buffer_file_length = bytes_range[1] - bytes_range[0] + 1
        loop.run_in_executor(None, self._create_buffer_file)
        self._headers = {'Range': 'bytes=%s-%s' % (self._bytes_range[0], self._bytes_range[1])}

    async def download(self):
        session = aiohttp.ClientSession(loop=self._loop, headers=self._headers)
        try:
            async with session.request('GET', self._path) as res:  # type: aiohttp.ClientResponse
                if res.status != 206:
                    raise PartDownloadError()
                hrh = res.headers  # type: CIMultiDictProxy
                # TODO: check headers.

                # Read content by chunks and write to the buffer file.
                while True:
                    chunk = await res.content.read(DEFAULT_CHUNK_SIZE)
                    if not chunk:
                        break
                    await self._write_chunk(chunk)
                await self._flush_and_release()

        except (aiohttp.ServerDisconnectedError, aiohttp.ClientResponseError):
            print("Connection error.")  # TODO: logging.
        finally:
            session.close()

    async def _write_chunk(self, chunk):
        # TODO: assert file is created before writing.
        await self._run_nonblocking(lambda: self._buffer_file.write(chunk))

    async def _flush_and_release(self):
        def flush_and_release():
            self._buffer_file.flush()
            self._buffer_file.close()
            del self._buffer_file

        await self._run_nonblocking(flush_and_release)

    async def _run_nonblocking(self, func):
        await self._loop.run_in_executor(None, lambda: func())

    def _create_buffer_file(self):
        f = open(self.buffer_file_path, 'xb')
        f.seek(self._buffer_file_length - 1)
        f.write(b'0')
        f.flush()
        f.seek(0)
        self._buffer_file = f

    def __repr__(self, *args, **kwargs):
        return '<Downloader: [{!s}-{!s}] {!r}>'.format(self._bytes_range[0], self._bytes_range[1], self._path)


class ParallelDownloader:
    def __init__(
            self,
            path: str,
            file_length: int,
            parallels: int = DEFAULT_PARALLELS,
            loop: AbstractEventLoop = None,
            buffer_dir: str = DEFAULT_BUFFER_DIR):
        assert parallels > 1
        self._path = path
        self._file_length = file_length
        self._parallels = parallels
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._filename = None  # type: str
        self._download_dir = os.path.join(buffer_dir, str(self._loop.time()).replace('.', '_'))
        self._create_download_dir()
        self._downloaders = []

    async def download(self):
        bytes_ranges = get_bytes_ranges(self._file_length, self._parallels)

        # Create a downloader for each bytes range.
        for i, bytes_range in enumerate(bytes_ranges):
            filename = '{:02}_{!s}-{!s}.tmp'.format(i, bytes_range[0], bytes_range[1])
            buffer_file_path = os.path.join(self._download_dir, filename)
            self._downloaders.append(Downloader(self._path, bytes_range, buffer_file_path, self._loop))

        # Start downloaders.
        downloads = []
        for downloader in self._downloaders:
            downloads.append(asyncio.ensure_future(downloader.download(), loop=self._loop))

        # Waiting for all downloads to complete.
        await asyncio.wait(downloads, loop=self._loop)

    async def read(self, callback: Callable[[bytearray], None]):
        chunk = bytearray(DEFAULT_CHUNK_SIZE)
        chunk_size = len(chunk)
        for downloader in self._downloaders:
            with open(downloader.buffer_file_path, 'rb') as file:
                while True:
                    r = await self._run_nonblocking(lambda: file.readinto(chunk))
                    if not r:
                        break
                    if r < chunk_size:
                        callback(memoryview(chunk)[:r].tobytes())
                    else:
                        callback(chunk)

    async def clear(self):
        await self._run_nonblocking(lambda: shutil.rmtree(self._download_dir))

    async def _run_nonblocking(self, func):
        return await self._loop.run_in_executor(None, lambda: func())

    def _create_download_dir(self):
        if not os.path.exists(self._download_dir):
            os.makedirs(self._download_dir)


class HttpRequestHandler(aiohttp.server.ServerHttpProtocol):
    def __init__(
            self, *, loop: AbstractEventLoop = None,
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
                await self.process_parallel(message, file_length)
                return
        # Process normally.
        self.log_message('%s %s' % (message.method, message.path))  # TODO: integrate aiohttp logging.
        await self.process_normally(message, req_data)

    async def process_normally(self, message: RawRequestMessage, req_data):
        session = aiohttp.ClientSession(headers=message.headers, loop=self._loop)
        try:
            async with session.request(message.method, message.path,
                                       data=req_data,
                                       allow_redirects=False) as host_resp:  # type: aiohttp.ClientResponse
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

    async def process_parallel(self, message, file_length):
        client_res = aiohttp.Response(self.writer, 200, http_version=message.version)
        client_res.add_header('CONTENT-LENGTH', str(file_length))
        client_res.send_headers()

        pd = ParallelDownloader(message.path, file_length, self.parallels, self._loop)
        try:
            await pd.download()
            await pd.read(lambda chunk: client_res.write(chunk))
        finally:
            await pd.clear()

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


def run():
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


if __name__ == '__main__':
    run()
