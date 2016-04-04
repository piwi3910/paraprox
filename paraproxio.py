#!/usr/bin/python

import sys

req_ver = (3, 5)
if sys.version_info < req_ver:
    print('Minimum Python/{0[0]}.{0[1]} required. You run this script with Python/{1[0]}.{1[1]}.'.format(
        req_ver, sys.version_info),
        file=sys.stderr)
    exit(1)

import asyncio
import logging
import os
import shutil

from asyncio import AbstractEventLoop
from typing import Tuple, Callable, Optional, List
from urllib.parse import urlparse

try:
    import aiohttp
    import aiohttp.hdrs as hdrs
    import aiohttp.server
    from aiohttp.multidict import CIMultiDictProxy
    from aiohttp.protocol import RawRequestMessage
    from aiohttp.streams import EmptyStreamReader
except ImportError as err:
    print(
        "Required module '{0}' not found. Try to run 'pip install {0}' to install it.".format(err.name),
        file=sys.stderr)
    exit(1)

DEFAULT_CHUNK_SIZE = 64 * 1024
DEFAULT_PARALLELS = 10

DEFAULT_WORKING_DIR = '.paraprox'
DEFAULT_BUFFER_DIR = os.path.join(DEFAULT_WORKING_DIR, 'buffer')
DEFAULT_LOGS_DIR = os.path.join(DEFAULT_WORKING_DIR, 'logs')
DEFAULT_SERVER_LOG_FILENAME = 'paraprox.server.log'
DEFAULT_ACCESS_LOG_FILENAME = 'paraprox.access.log'

server_logger = logging.getLogger('paraprox.server')
access_logger = logging.getLogger('paraprox.access')

files_to_parallel = ['.iso', '.zip', '.rpm', '.gz']


def need_file_to_parallel(path: str) -> bool:
    pr = urlparse(path)  # type: ParseResult
    path, ext = os.path.splitext(pr.path)
    return ext.lower() in files_to_parallel


def get_bytes_ranges(length: int, parts: int) -> List[Tuple[int, int]]:
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

    def check_request(self, message: RawRequestMessage):
        if message.method == hdrs.METH_CONNECT:
            self.handle_error(status=405, message=message)
            raise UnsupportedError("Method '%s' is not supported." % message.method)

    async def handle_request(self, message: RawRequestMessage, payload):
        now = self._loop.time()

        self.check_request(message)
        self.keep_alive(True)

        # Try to process parallel.
        response = await self.process_parallel(message, payload)

        # Otherwise process normally.
        if not response:
            response = await self.process_normally(message, payload)

        self.log_access(message, None, response, self._loop.time() - now)

    async def process_normally(self, message: RawRequestMessage, payload):
        """Process request normally."""
        req_data = payload if not isinstance(payload, EmptyStreamReader) else None

        # Request from a host.
        session = aiohttp.ClientSession(headers=message.headers, loop=self._loop)
        try:
            async with session.request(message.method, message.path,
                                       data=req_data,
                                       allow_redirects=False) as host_resp:  # type: aiohttp.ClientResponse
                client_res = aiohttp.Response(
                    self.writer, host_resp.status, http_version=message.version)

                # Process host response headers.
                for name, value in host_resp.headers.items():
                    if name == hdrs.CONTENT_ENCODING:
                        continue
                    if name == hdrs.CONTENT_LENGTH:
                        continue
                    if name == hdrs.TRANSFER_ENCODING:
                        if value.lower() == 'chunked':
                            client_res.enable_chunked_encoding()
                    client_res.add_header(name, value)

                # Send headers to the client.
                client_res.send_headers()

                # Send a payload.
                read = 0
                while True:
                    chunk = await host_resp.content.read(DEFAULT_CHUNK_SIZE)
                    read += len(chunk)
                    if not chunk:
                        break
                    client_res.write(chunk)

                if client_res.chunked or client_res.autochunked():
                    client_res.write_eof()
        finally:
            session.close()
        return client_res

    async def process_parallel(self, message: RawRequestMessage, payload) -> aiohttp.Response:
        """Try process a request parallel. Returns True in case of processed parallel, otherwise False."""
        # Checking the opportunity of parallel downloading.
        if message.method != hdrs.METH_GET or not need_file_to_parallel(message.path):
            return None
        head = await self.get_file_head(message.path)
        if head is None:
            return None
        accept_ranges = head.get(hdrs.ACCEPT_RANGES).lower() == 'bytes'
        if not accept_ranges:
            return None
        content_length = head.get(hdrs.CONTENT_LENGTH)
        if content_length is None:
            return None
        content_length = int(content_length)
        if content_length <= 0:
            return None

        # All checks pass, start a parallel downloading.
        # TODO: log as access, not as debug.
        self.log_debug('PARALLEL GET %s [%s bytes]' % (message.path, content_length))

        # Get additional file info.
        content_type = head.get(hdrs.CONTENT_TYPE)

        # Prepare a response to a client.
        client_res = aiohttp.Response(self.writer, 200, http_version=message.version)
        client_res.add_header(hdrs.CONTENT_LENGTH, str(content_length))
        if content_type:
            client_res.add_header(hdrs.CONTENT_TYPE, content_type)
        client_res.send_headers()

        pd = ParallelDownloader(message.path, content_length, self.parallels, self._loop)
        try:
            await pd.download()
            await pd.read(lambda chunk: client_res.write(chunk))
        finally:
            await pd.clear()

        return client_res

    async def get_file_head(self, path: str) -> Optional[CIMultiDictProxy]:
        """Make a HEAD request to get a 'content-length' and 'accept-ranges' headers."""
        session = aiohttp.ClientSession(loop=self._loop)
        try:
            async with session.request(hdrs.METH_HEAD, path) as res:  # type: aiohttp.ClientResponse
                return res.headers
        except (aiohttp.ServerDisconnectedError, aiohttp.ClientResponseError):
            self.log_debug("Could not get a HEAD for the {!r}.".format(path))
        finally:
            session.close()
        return None

    def get_client_address(self):
        address, port = self.transport.get_extra_info('peername')
        return '%s:%s' % (address, port)


class ParaproxError(Exception):
    pass


class PartDownloadError(ParaproxError):
    pass


class UnsupportedError(ParaproxError):
    pass


def setup_dirs(buffer_dir: str, logs_dir: str):
    os.makedirs(buffer_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)


def setup_logging(logs_dir: str, server_log_filename: str, access_log_filename: str):
    server_logger.setLevel(logging.DEBUG)
    access_logger.setLevel(logging.DEBUG)

    # stderr handler.
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.DEBUG)
    server_logger.addHandler(ch)
    access_logger.addHandler(ch)

    # Server log file handler.
    slfp = os.path.join(logs_dir, server_log_filename)
    slfh = logging.FileHandler(slfp)
    slfh.setLevel(logging.DEBUG)
    server_logger.addHandler(slfh)

    # Access log file handler.
    alfp = os.path.join(logs_dir, access_log_filename)
    alfh = logging.FileHandler(alfp)
    alfh.setLevel(logging.DEBUG)
    access_logger.addHandler(alfh)


def run():
    setup_dirs(DEFAULT_BUFFER_DIR, DEFAULT_LOGS_DIR)
    setup_logging(DEFAULT_LOGS_DIR, DEFAULT_SERVER_LOG_FILENAME, DEFAULT_ACCESS_LOG_FILENAME)
    loop = asyncio.get_event_loop()

    def create_http_request_handler():
        return HttpRequestHandler(
            loop=loop,
            logger=server_logger,
            access_log=access_logger,
            debug=True,
            keep_alive=75)

    f = loop.create_server(create_http_request_handler, '127.0.0.1', '8880')

    srv = loop.run_until_complete(f)
    print('serving on', srv.sockets[0].getsockname())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    run()
