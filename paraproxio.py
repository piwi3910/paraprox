#!/usr/bin/python

__version__ = '1.0'

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
import argparse

from asyncio import AbstractEventLoop, Future
from asyncio.futures import CancelledError
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

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 8880

DEFAULT_CHUNK_DOWNLOAD_TIMEOUT = 10
DEFAULT_CHUNK_SIZE = 64 * 1024
DEFAULT_PARALLELS = 10

DEFAULT_WORKING_DIR = '.paraprox'
DEFAULT_BUFFER_DIR = os.path.join(DEFAULT_WORKING_DIR, 'buffer')
DEFAULT_LOGS_DIR = os.path.join(DEFAULT_WORKING_DIR, 'logs')
DEFAULT_SERVER_LOG_FILENAME = 'paraprox.server.log'
DEFAULT_ACCESS_LOG_FILENAME = 'paraprox.access.log'

NOT_STARTED = 'NOT STARTED'
DOWNLOADING = 'DOWNLOADING'
DOWNLOADED = 'DOWNLOADED'
CANCELLED = 'CANCELLED'

_DOWNLOADER_STATES = {NOT_STARTED, DOWNLOADING, DOWNLOADED, CANCELLED}

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


class RangeDownloader:
    def __init__(
            self,
            path: str,
            bytes_range: Tuple[int, int],
            buffer_file_path,
            *,
            loop: AbstractEventLoop = None,
            chunk_size=DEFAULT_CHUNK_SIZE,
            chunk_download_timeout=DEFAULT_CHUNK_DOWNLOAD_TIMEOUT):
        self._path = path
        self._bytes_range = bytes_range
        self.buffer_file_path = buffer_file_path
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._chunk_size = chunk_size
        self._chunk_download_timeout = chunk_download_timeout
        self._buffer_file_length = bytes_range[1] - bytes_range[0] + 1
        self._headers = {'Range': 'bytes={0[0]!s}-{0[1]!s}'.format(self._bytes_range)}
        self._bytes_downloaded = 0
        self._state = NOT_STARTED

    @property
    def state(self) -> str:
        return self._state

    @property
    def bytes_downloaded(self):
        return self._bytes_downloaded

    async def download(self) -> str:
        # Prepare an empty buffer file.
        await self._loop.run_in_executor(None, self._create_buffer_file)

        # Create client session for downloading a file part from a host.
        session = aiohttp.ClientSession(loop=self._loop, headers=self._headers)
        try:
            # Request a host for a file part.
            async with session.request('GET', self._path) as res:  # type: aiohttp.ClientResponse
                if res.status != 206:
                    raise PartDownloadError('Expected status code 206, but {!s} received.', res.status)
                hrh = res.headers  # type: CIMultiDictProxy
                # TODO: check headers.

                # Read content by chunks and write to the buffer file.
                self._state = DOWNLOADING
                while self._state is DOWNLOADING:
                    with aiohttp.Timeout(self._chunk_download_timeout):
                        chunk = await res.content.read(self._chunk_size)
                        self._bytes_downloaded += len(chunk)
                        if not chunk:
                            self._state = DOWNLOADED
                            break
                        await self._write_chunk(chunk)
                await self._flush_and_release()

        except Exception as exc:
            self.cancel()
        finally:
            session.close()
            return self._state

    def cancel(self):
        if self._state != DOWNLOADING:
            return
        self._state = CANCELLED

    async def _write_chunk(self, chunk):
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
        return '<RangeDownloader: [{0[0]!s}-{0[1]!s}] {!r}>'.format(self._bytes_range, self._path)


class ParallelDownloader:
    def __init__(
            self,
            path: str,
            file_length: int,
            *,
            parallels: int = DEFAULT_PARALLELS,
            chunk_size: int = DEFAULT_CHUNK_SIZE,
            loop: AbstractEventLoop = None,
            buffer_dir: str = DEFAULT_BUFFER_DIR):
        assert parallels > 1

        self._path = path
        self._file_length = file_length
        self._parallels = parallels
        self._chunk_size = chunk_size
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        self._filename = None  # type: str
        self._download_dir = os.path.join(buffer_dir, str(self._loop.time()).replace('.', '_'))
        self._downloaders = []  # type: List[RangeDownloader]
        self._downloads = []  # type: List[Future]
        self._state = NOT_STARTED

        self._create_download_dir()

    @property
    def state(self) -> str:
        return self._state

    @property
    def downloaders(self):
        return self._downloaders

    async def download(self):
        assert self._state == NOT_STARTED
        self._state = DOWNLOADING

        # Calculate bytes ranges.
        bytes_ranges = get_bytes_ranges(self._file_length, self._parallels)

        # Create a downloader for each bytes range.
        for i, bytes_range in enumerate(bytes_ranges):
            filename = '{idx:02}_{range[0]!s}-{range[1]!s}.tmp'.format(idx=i, range=bytes_range)
            buffer_file_path = os.path.join(self._download_dir, filename)
            self._downloaders.append(RangeDownloader(
                self._path, bytes_range, buffer_file_path, loop=self._loop, chunk_size=self._chunk_size))

        # Start downloaders.
        for downloader in self._downloaders:
            self._downloads.append(asyncio.ensure_future(downloader.download(), loop=self._loop))

        # Waiting for all downloads to complete.
        try:
            pending = set(self._downloads)
            while self._state is DOWNLOADING and pending:
                done, pending = await asyncio.wait(pending, loop=self._loop, return_when=asyncio.FIRST_COMPLETED)
                for dd in done:  # type: Future
                    if dd.result() is not DOWNLOADED:
                        raise CancelledError()

        except Exception as ex:
            self.cancel()
            raise
        else:
            # OK. All done.
            self._state = DOWNLOADED

    async def read(self, callback: Callable[[bytearray], None]):
        chunk = bytearray(self._chunk_size)
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

    def cancel(self):
        if self._state != DOWNLOADING:
            return
        self._state = CANCELLED
        for downloader in self._downloaders:  # type: RangeDownloader
            downloader.cancel()

    async def clear(self):
        if self._state not in (DOWNLOADED, CANCELLED):
            return
        await self._clear()

    async def _clear(self):
        await self._run_nonblocking(lambda: shutil.rmtree(self._download_dir, ignore_errors=True))

    async def _run_nonblocking(self, func):
        return await self._loop.run_in_executor(None, lambda: func())

    def _create_download_dir(self):
        if not os.path.exists(self._download_dir):
            os.makedirs(self._download_dir)

    def __repr__(self, *args, **kwargs):
        return '<ParallelDownloader: {!r}>'.format(self._path)


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
            chunk_size: int = DEFAULT_CHUNK_SIZE,
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
        self._parallels = parallels
        self._chunk_size = chunk_size

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
                while True:
                    chunk = await host_resp.content.read(self._chunk_size)
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
        self.log_debug("PARALLEL GET {!r} [{!s} bytes]".format(message.path, content_length))

        # Get additional file info.
        content_type = head.get(hdrs.CONTENT_TYPE)

        # Prepare a response to a client.
        client_res = aiohttp.Response(self.writer, 200, http_version=message.version)
        client_res.add_header(hdrs.CONTENT_LENGTH, str(content_length))
        if content_type:
            client_res.add_header(hdrs.CONTENT_TYPE, content_type)
        client_res.send_headers()

        pd = ParallelDownloader(message.path, content_length,
                                parallels=self._parallels, chunk_size=self._chunk_size, loop=self._loop)
        try:
            await pd.download()
            await pd.read(lambda chunk: client_res.write(chunk))
            client_res.write_eof()
        except Exception as exc:
            self.log_debug("CANCELLED PARALLEL GET {!r}.".format(message.path))
            raise
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


def setup_dirs(*dirs):
    for d in dirs:
        os.makedirs(d, exist_ok=True)


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


def get_args():
    parser = argparse.ArgumentParser(prog="paraprox",
                                     description="An HTTP proxy with a parallel downloading of big files.")
    parser.add_argument("-H", "--host", type=str, default=DEFAULT_HOST, help="host address")
    parser.add_argument("-P", "--port", type=int, default=DEFAULT_PORT, help="port")
    parser.add_argument("--parallels", type=int, default=DEFAULT_PARALLELS, help="parallel downloads of a big file")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="chunk size")
    parser.add_argument("--buffer-dir", type=str, default=DEFAULT_BUFFER_DIR, help="buffer dir")
    parser.add_argument("--logs-dir", type=str, default=DEFAULT_LOGS_DIR, help="logs dir")
    parser.add_argument("--debug", action="store_true", help="enable debug information in the stdout")
    parser.add_argument("--version", action="version", version="Paraprox/" + __version__)
    return parser.parse_args()


def run():
    args = get_args()

    setup_dirs(args.buffer_dir, args.logs_dir)
    setup_logging(args.logs_dir, DEFAULT_SERVER_LOG_FILENAME, DEFAULT_ACCESS_LOG_FILENAME)

    loop = asyncio.get_event_loop()

    def create_http_request_handler():
        return HttpRequestHandler(
            loop=loop,
            logger=server_logger,
            access_log=access_logger,
            debug=args.debug,
            parallels=args.parallels,
            chunk_size=args.chunk_size,
            keep_alive=75)

    f = loop.create_server(create_http_request_handler, args.host, args.port)

    srv = loop.run_until_complete(f)
    print('Paraprox serving on', srv.sockets[0].getsockname())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    run()
