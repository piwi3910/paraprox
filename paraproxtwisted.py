# Python based multi-threaded http proxy
# http request must do a content length evaluation
# if content is smaller: then predefined value then normal http get and response on proxy port
# if content is larger: then divide the content length in predefined chunks and process all chunks separate in parallel
# (byte-range request)
# After all chunks are downloaded reconstruct the full content and pass through the proxy port.
# If byte-range requests are not possible,  fall back to single stream get
#
# All this must use regex to define if a request must be single or accelerated:
# Like zip, rpm,iso -> try to accelerate with byte-code request split
# all others -> single stream

import sys

from twisted.internet import reactor
from twisted.internet.interfaces import IReactorCore, IReactorTCP
from twisted.internet.endpoints import TCP4ClientEndpoint, TCP4ServerEndpoint
from twisted.internet.protocol import ClientFactory
from twisted.python import log
from twisted.python.compat import urllib_parse
from twisted.web import http
from twisted.web.http import HTTPClient, Request, HTTPChannel


class ParaproxClient(HTTPClient):
    _finished = False

    def __init__(self, command, rest, version, headers, data, father):
        self.father = father
        self.command = command
        self.rest = rest
        if b"proxy-connection" in headers:
            del headers[b"proxy-connection"]
        headers[b"connection"] = b"close"
        headers.pop(b'keep-alive', None)
        self.headers = headers
        self.data = data

    def connectionMade(self):
        self.sendCommand(self.command, self.rest)
        for header, value in self.headers.items():
            self.sendHeader(header, value)
        self.endHeaders()
        self.transport.write(self.data)

    def handleStatus(self, version, code, message):
        self.father.setResponseCode(int(code), message)

    def handleHeader(self, key, value):
        # t.web.server.Request sets default values for these headers in its
        # 'process' method. When these headers are received from the remote
        # server, they ought to override the defaults, rather than append to
        # them.
        if key.lower() in [b'server', b'date', b'content-type']:
            self.father.responseHeaders.setRawHeaders(key, [value])
        else:
            self.father.responseHeaders.addRawHeader(key, value)

    def handleResponsePart(self, buffer):
        self.father.write(buffer)

    def handleResponseEnd(self):
        """
        Finish the original request, indicating that the response has been
        completely written to it, and disconnect the outgoing transport.
        """
        if not self._finished:
            self._finished = True
            self.father.finish()
            self.transport.loseConnection()


class ParaproxClientFactory(ClientFactory):
    """
    Used by ProxyRequest to implement a simple web proxy.
    """

    protocol = ParaproxClient

    def __init__(self, command, rest, version, headers, data, father):
        self.father = father
        self.command = command
        self.rest = rest
        self.headers = headers
        self.data = data
        self.version = version

    def buildProtocol(self, addr):
        return self.protocol(self.command, self.rest, self.version,
                             self.headers, self.data, self.father)

    def clientConnectionFailed(self, connector, reason):
        """
        Report a connection failure in a response to the incoming request as
        an error.
        """
        self.father.setResponseCode(501, b"Gateway error")
        self.father.responseHeaders.addRawHeader(b"Content-Type", b"text/html")
        self.father.write(b"<H1>Could not connect</H1>")
        self.father.finish()

    def clientConnectionLost(self, connector, reason):
        pass


class ParaproxRequest(Request):
    factories = {b'http': ParaproxClientFactory}
    ports = {b'http': 80}

    def __init__(self, channel, queued, reactor: IReactorTCP = reactor):
        Request.__init__(self, channel, queued)
        self.reactor = reactor
        self.client_endpoint = None

    def got_protocol(self, p):
        print(p)

    def process(self):
        log.msg('Processing: %s' % self.uri)
        parsed = urllib_parse.urlparse(self.uri)

        protocol = parsed[0]
        if protocol == b'':
            protocol = b'http'
        elif protocol == b'https':
            raise ConnectionRefusedError('The https protocol is not supported.')

        host = parsed[1].decode('ascii')
        if host is b'':
            raise ConnectionRefusedError('Host is empty.')

        port = self.ports[protocol]
        if ':' in host:
            host, port = host.split(':')
            port = int(port)

        rest = urllib_parse.urlunparse((b'', b'') + parsed[2:])
        if not rest:
            rest = rest + b'/'

        headers = self.getAllHeaders().copy()
        if b'host' not in headers:
            headers[b'host'] = host.encode('ascii')

        self.content.seek(0, 0)
        content = self.content.read()

        new_c_factory = self.factories[protocol]
        c_factory = new_c_factory(self.method, rest, self.clientproto, headers, content, self)

        self.client_endpoint = TCP4ClientEndpoint(self.reactor, host, port)
        d = self.client_endpoint.connect(c_factory)
        d.addCallback(lambda: self.got_protocol)

    def connectionLost(self, reason):
        # TODO: Disconnect incoming client connection.
        super().connectionLost(reason)


class ParaproxChannel(HTTPChannel):
    requestFactory = ParaproxRequest


class ProxyFactory(http.HTTPFactory):
    def buildProtocol(self, addr):
        return ParaproxChannel()


class Paraprox:
    def __init__(self, reactor: IReactorCore = reactor):
        self.reactor = reactor

    def start(self):
        endpoint = TCP4ServerEndpoint(reactor, 8880)
        endpoint.listen(ProxyFactory())
        self.reactor.run()

    def stop(self):
        self.reactor.stop()


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    proxy = Paraprox()
    proxy.start()
