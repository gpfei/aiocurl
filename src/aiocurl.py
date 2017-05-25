""" HTTP client using with asyncio.
This is a wrapper of libcurl.

References:
  [1] https://curl.haxx.se/libcurl/c/multi-uv.html
  [2] https://github.com/tornadoweb/tornado/blob/master/tornado/curl_httpclient.py

"""
import asyncio
from collections import namedtuple
import functools
import io
import logging
import threading
import time
import typing

import httptools
import multidict
import pycurl
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import httputil


logging.basicConfig(level=logging.DEBUG)
curl_log = logging.getLogger('aiocurl')

# global multi instance
g_multi = pycurl.CurlMulti()
# g_multi.setopt(pycurl.M_MAX_HOST_CONNECTIONS, 100)
# global fd collection
g_fds = dict()


class HTTPRequest(object):
    """HTTP client request object."""

    # Default values for HTTPRequest parameters.
    # Merged with the values on the request object by AsyncHTTPClient
    # implementations.
    _DEFAULTS = dict(
        connect_timeout=20.0,
        request_timeout=20.0,
        follow_redirects=True,
        max_redirects=5,
        decompress_response=True,
        proxy_password='',
        allow_nonstandard_methods=False,
        validate_cert=True)

    def __init__(self, url, method="GET", headers=None, body=None,
                 auth_username=None, auth_password=None, auth_mode=None,
                 connect_timeout=None, request_timeout=None,
                 if_modified_since=None, follow_redirects=None,
                 max_redirects=None, user_agent=None, use_gzip=None,
                 network_interface=None, streaming_callback=None,
                 header_callback=None, prepare_curl_callback=None,
                 proxy_host=None, proxy_port=None, proxy_username=None,
                 proxy_password=None, proxy_auth_mode=None,
                 allow_nonstandard_methods=None, validate_cert=None,
                 ca_certs=None, allow_ipv6=None, client_key=None,
                 client_cert=None, body_producer=None,
                 expect_100_continue=False, decompress_response=None,
                 ssl_options=None):
        self.headers = headers
        if if_modified_since:
            pass
            # self.headers["If-Modified-Since"] = httputil.format_timestamp(
            #     if_modified_since)
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.proxy_auth_mode = proxy_auth_mode
        self.url = url
        self.method = method
        self.body = body
        self.body_producer = body_producer
        self.auth_username = auth_username
        self.auth_password = auth_password
        self.auth_mode = auth_mode
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.follow_redirects = follow_redirects
        self.max_redirects = max_redirects
        self.user_agent = user_agent
        if decompress_response is not None:
            self.decompress_response = decompress_response
        else:
            self.decompress_response = use_gzip
        self.network_interface = network_interface
        self.streaming_callback = streaming_callback
        self.header_callback = header_callback
        if self.streaming_callback or self.header_callback:
            raise NotImplementedError
        self.prepare_curl_callback = prepare_curl_callback
        self.allow_nonstandard_methods = allow_nonstandard_methods
        self.validate_cert = validate_cert
        self.ca_certs = ca_certs
        self.allow_ipv6 = allow_ipv6
        self.client_key = client_key
        self.client_cert = client_cert
        self.ssl_options = ssl_options
        self.expect_100_continue = expect_100_continue
        self.start_time = time.time()

    @property
    def headers(self):
        return self._headers

    @headers.setter
    def headers(self, value):
        if value is None:
            self._headers = {}
        else:
            self._headers = value

    @property
    def body(self):
        return self._body

    @body.setter
    def body(self, value):
        self._body = value

    @property
    def body_producer(self):
        return self._body_producer

    @body_producer.setter
    def body_producer(self, value):
        self._body_producer = value

    @property
    def streaming_callback(self):
        return self._streaming_callback

    @streaming_callback.setter
    def streaming_callback(self, value):
        self._streaming_callback = value

    @property
    def header_callback(self):
        return self._header_callback

    @header_callback.setter
    def header_callback(self, value):
        self._header_callback = value

    @property
    def prepare_curl_callback(self):
        return self._prepare_curl_callback

    @prepare_curl_callback.setter
    def prepare_curl_callback(self, value):
        self._prepare_curl_callback = value


class HTTPResponse(object):
    def __init__(self, request, code, raw_headers=None, buffer=None,
                 effective_url=None, error=None, request_time=None,
                 time_info=None, reason=None):
        if isinstance(request, _RequestProxy):
            self.request = request.request
        else:
            self.request = request
        self.status = self.code = code
        self._reason = reason
        # headers
        if raw_headers is not None:
            self.raw_headers = raw_headers
        else:
            self.raw_headers = ''
        self._headers = None
        #
        self.buffer = buffer
        self._body = None
        if effective_url is None:
            self.effective_url = request.url
        else:
            self.effective_url = effective_url
        if error is None:
            if self.code < 200 or self.code >= 300:
                self.error = HTTPError(self.code, message=self.reason,
                                       response=self)
            else:
                self.error = None
        else:
            self.error = error
        self.request_time = request_time
        self.time_info = time_info or {}

    @property
    def body(self):
        if self.buffer is None:
            return None
        elif self._body is None:
            self._body = self.buffer.getvalue()

        return self._body

    @property
    def reason(self):
        if not self._reason:
            self._reason = self.headers.get("X-Http-Reason", 'Unknown')
        return self._reason

    @property
    def headers(self):
        if self._headers is None:
            _headers = httputil.HTTPHeaders()
            if self.raw_headers:
                _headers = httputil.HTTPHeaders.parse(self.raw_headers.decode())
            self._headers = _headers
        return self._headers

    def rethrow(self):
        """If there was an error on the request, raise an `HTTPError`."""
        if self.error:
            raise self.error

    def __repr__(self):
        args = ",".join("%s=%r" % i for i in sorted(self.__dict__.items()))
        return "%s(%s)" % (self.__class__.__name__, args)


class _RequestProxy(object):
    """Combines an object with a dictionary of defaults.
    Used internally by AsyncHTTPClient implementations.
    """
    def __init__(self, request, defaults):
        self.request = request
        self.defaults = defaults

    def __getattr__(self, name):
        request_attr = getattr(self.request, name)
        if request_attr is not None:
            return request_attr
        elif self.defaults is not None:
            return self.defaults.get(name, None)
        else:
            return None


class HTTPError(Exception):
    """Exception thrown for an unsuccessful HTTP request.
    Attributes:
    * ``code`` - HTTP error integer error code, e.g. 404.  Error code 599 is
      used when no HTTP response was received, e.g. for a timeout.
    * ``response`` - `HTTPResponse` object, if any.
    Note that if ``follow_redirects`` is False, redirects become HTTPErrors,
    and you can look at ``error.response.headers['Location']`` to see the
    destination of the redirect.
    """
    def __init__(self, code, message=None, response=None):
        self.code = code
        self.message = message # or httputil.responses.get(code, "Unknown")
        self.response = response
        super(HTTPError, self).__init__(code, message, response)

    def __str__(self):
        return "HTTP %d: %s" % (self.code, self.message)

    # There is a cyclic reference between self and self.response,
    # which breaks the default __repr__ implementation.
    # (especially on pypy, which doesn't have the same recursion
    # detection as cpython).
    __repr__ = __str__


class CurlError(HTTPError):
    def __init__(self, errno, message):
        HTTPError.__init__(self, 599, message)
        self.errno = errno


CurlInfo = namedtuple(
    'CurlInfo', [
        'future', 'request', 'buffer', 'headers_buffer', 'curl_start_time',
    ])


class ClientSession():

    def __init__(self, loop=None, separated=False, max_clients=10):
        if loop is None:
            loop = asyncio.get_event_loop()
        self.loop = loop
        if separated:
            self._multi = pycurl.CurlMulti()
            self._fds = {}
        else:
            self._multi = g_multi
            self._fds = g_fds

        self._multi.setopt(pycurl.M_TIMERFUNCTION, self._set_timeout)
        self._multi.setopt(pycurl.M_SOCKETFUNCTION, self._handle_socket)

        _share = pycurl.CurlShare()
        _share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)
        _share.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)
        self._share = _share

        self._curls = [self._curl_create() for i in range(max_clients)]
        self._free_list = asyncio.Queue()
        for _curl in self._curls:
            self._free_list.put_nowait(_curl)

        self._timeout = None
        self._closed = False

    def __del__(self):
        if not self.closed:
            self.close()

    def close(self):
        if not self._closed:
            self._closed = True
            self._share.close()
            for curl in self._curls:
                curl.close()

    def _set_timeout(self, msecs):
        if self._timeout is not None:
            self._timeout.cancel()

        self._timeout = self.loop.call_at(
            self.loop.time() + msecs / 1000.0, self._on_timeout)

    def _on_timeout(self):
        self._timeout = None
        while True:
            try:
                ret, num_handles = self._multi.socket_action(
                    pycurl.SOCKET_TIMEOUT, 0)
            except pycurl.error as exc:
                curl_log.exception('Failed to perform socket_action.')
                ret = exc.args[0]

            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break

        self._check_multi_info()

    def _check_multi_info(self):
        while True:
            num_q, ok_list, err_list = self._multi.info_read()
            for curl in ok_list:
                self._finish(curl)
            for curl, errnum, errmsg in err_list:
                self._finish(curl, errnum, errmsg)
            if num_q == 0:
                break

    def _handle_socket(self, event, fd, multi, data):
        if event in (pycurl.POLL_IN, pycurl.POLL_OUT, pycurl.POLL_INOUT):
            if fd in self._fds:
                self.loop.remove_reader(fd)
                self.loop.remove_writer(fd)
            self.loop.add_reader(fd, self._handle_events, fd, event)
            self.loop.add_writer(fd, self._handle_events, fd, event)
            self._fds[fd] = event
        elif event == pycurl.POLL_REMOVE:
            if fd in self._fds:
                self.loop.remove_reader(fd)
                self.loop.remove_writer(fd)
                del self._fds[fd]

    def _handle_events(self, fd, events):
        action = 0
        if events & pycurl.POLL_IN or events & pycurl.POLL_INOUT:
            action |= pycurl.CSELECT_IN
        if events & pycurl.POLL_OUT or events & pycurl.POLL_INOUT:
            action |= pycurl.CSELECT_OUT

        while True:
            try:
                ret, num_handles = self._multi.socket_action(fd, action)
            except pycurl.error as exc:
                curl_log.exception('Failed to perform socket_action.')
                ret = exc.args[0]

            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break

        self._check_multi_info()

    def _finish(self, curl, curl_error=None, curl_message=None):
        info = curl.info
        curl.info = None
        self._multi.remove_handle(curl)
        self._free_list.put_nowait(curl)
        buf = info.buffer
        if curl_error:
            error = CurlError(curl_error, curl_message)
            code = error.code
            effective_url = None
            buf.close()
            buf = None
        else:
            error = None
            code = curl.getinfo(pycurl.HTTP_CODE)
            effective_url = curl.getinfo(pycurl.EFFECTIVE_URL)
            buf.seek(0)

        time_info = dict(
            queue=info.curl_start_time - info.request.start_time,
            namelookup=curl.getinfo(pycurl.NAMELOOKUP_TIME),
            connect=curl.getinfo(pycurl.CONNECT_TIME),
            pretransfer=curl.getinfo(pycurl.PRETRANSFER_TIME),
            starttransfer=curl.getinfo(pycurl.STARTTRANSFER_TIME),
            total=curl.getinfo(pycurl.TOTAL_TIME),
            redirect=curl.getinfo(pycurl.REDIRECT_TIME),
        )
        try:
            result = HTTPResponse(
                request=info.request, code=code,
                raw_headers=info.headers_buffer.getvalue(),
                buffer=buf, effective_url=effective_url, error=error,
                request_time=time.time() - info.curl_start_time,
                time_info=time_info)
            info.future.set_result(result)
        except Exception:
            curl_log.exception('Failed to run callback.')

    async def fetch(self, request):
        future = self.loop.create_future()

        buf = io.BytesIO()
        headers_buf = io.BytesIO()

        start_time = time.monotonic()
        curl = await self._free_list.get()
        curl.info = CurlInfo(
            future, request, buf, headers_buf, start_time)

        request = _RequestProxy(request, dict(HTTPRequest._DEFAULTS))

        try:
            self._curl_setup_request(
                curl, request, curl.info.buffer,
                curl.info.headers_buffer)
        except Exception as e:
            curl_log.exception('Failed to setup request.')
            res = HTTPResponse(
                request=request,
                status=599,
                error=e)
            future.set_result(res)
            await self._free_list.put(curl)
        else:
            self._multi.add_handle(curl)
            self._set_timeout(0)

        try:
            return await future
        except Exception:
            future.cancel()

    def _curl_create(self):
        curl = pycurl.Curl()
        if curl_log.isEnabledFor(logging.DEBUG):
            pass
            # curl.setopt(pycurl.VERBOSE, 1)
            # curl.setopt(pycurl.DEBUGFUNCTION, self._curl_debug)

        # PROTOCOLS first appeared in pycurl 7.19.5 (2014-07-12)
        if hasattr(pycurl, 'PROTOCOLS'):
            curl.setopt(
                pycurl.PROTOCOLS, pycurl.PROTO_HTTP | pycurl.PROTO_HTTPS)
            curl.setopt(
                pycurl.REDIR_PROTOCOLS, pycurl.PROTO_HTTP | pycurl.PROTO_HTTPS)
        return curl

    def _curl_setup_request(self, curl: pycurl.Curl, request: HTTPRequest,
                            buf: io.BytesIO, headers_buf: io.BytesIO):
        curl.setopt(pycurl.URL, request.url.encode())
        curl.setopt(pycurl.WRITEFUNCTION, buf.write)

        # libcurl's magic "Expect: 100-continue" behavior causes delays
        # with servers that don't support it (which include, among others,
        # Google's OpenID endpoint).  Additionally, this behavior has
        # a bug in conjunction with the curl_multi_socket_action API
        # (https://sourceforge.net/tracker/?func=detail&atid=100976&aid=3039744&group_id=976),
        # which increases the delays.  It's more trouble than it's worth,
        # so just turn off the feature (yes, setting Expect: to an empty
        # value is the official way to disable this)
        if "Expect" not in request.headers:
            request.headers["Expect"] = ""

        # libcurl adds Pragma: no-cache by default; disable that too
        if "Pragma" not in request.headers:
            request.headers["Pragma"] = ""

        curl.setopt(pycurl.HTTPHEADER,
                    ["{}: {}".format(k, v).encode()
                     for k, v in request.headers.items()])

        curl.setopt(pycurl.HEADERFUNCTION,
                    functools.partial(self._curl_header_callback,
                                      headers_buf, request.header_callback))
        curl.setopt(pycurl.WRITEFUNCTION, buf.write)
        curl.setopt(pycurl.FOLLOWLOCATION, request.follow_redirects)
        curl.setopt(pycurl.MAXREDIRS, request.max_redirects)
        curl.setopt(pycurl.CONNECTTIMEOUT_MS, int(1000 * request.connect_timeout))
        curl.setopt(pycurl.TIMEOUT_MS, int(1000 * request.request_timeout))
        if request.user_agent:
            curl.setopt(pycurl.USERAGENT, request.user_agent.encode())
        else:
            curl.setopt(pycurl.USERAGENT, b"Mozilla/5.0 (compatible; aiocurl)")
        if request.network_interface:
            curl.setopt(pycurl.INTERFACE, request.network_interface)
        if request.decompress_response:
            curl.setopt(pycurl.ENCODING, b"gzip,deflate")
        else:
            curl.setopt(pycurl.ENCODING, b"none")

        if request.proxy_host and request.proxy_port:
            curl.setopt(pycurl.PROXY, request.proxy_host.encode())
            curl.setopt(pycurl.PROXYPORT, request.proxy_port)
            if request.proxy_username:
                credentials = '{}:{}'.format(
                    request.proxy_username, request.proxy_password)
                curl.setopt(pycurl.PROXYUSERPWD, credentials.encode())

            if (request.proxy_auth_mode is None or
                    request.proxy_auth_mode == "basic"):
                curl.setopt(pycurl.PROXYAUTH, pycurl.HTTPAUTH_BASIC)
            elif request.proxy_auth_mode == "digest":
                curl.setopt(pycurl.PROXYAUTH, pycurl.HTTPAUTH_DIGEST)
            else:
                raise ValueError(
                    "Unsupported proxy_auth_mode %s" % request.proxy_auth_mode)
        else:
            curl.setopt(pycurl.PROXY, b'')
            curl.unsetopt(pycurl.PROXYUSERPWD)
        if request.validate_cert:
            curl.setopt(pycurl.SSL_VERIFYPEER, 1)
            curl.setopt(pycurl.SSL_VERIFYHOST, 2)
        else:
            curl.setopt(pycurl.SSL_VERIFYPEER, 0)
            curl.setopt(pycurl.SSL_VERIFYHOST, 0)
        if request.ca_certs is not None:
            curl.setopt(pycurl.CAINFO, request.ca_certs)
        else:
            # There is no way to restore pycurl.CAINFO to its default value
            # (Using unsetopt makes it reject all certificates).
            # I don't see any way to read the default value from python so it
            # can be restored later.  We'll have to just leave CAINFO untouched
            # if no ca_certs file was specified, and require that if any
            # request uses a custom ca_certs file, they all must.
            pass

        if request.allow_ipv6 is False:
            # Curl behaves reasonably when DNS resolution gives an ipv6 address
            # that we can't reach, so allow ipv6 unless the user asks to disable.
            curl.setopt(pycurl.IPRESOLVE, pycurl.IPRESOLVE_V4)
        else:
            curl.setopt(pycurl.IPRESOLVE, pycurl.IPRESOLVE_WHATEVER)

        # Set the request method through curl's irritating interface which makes
        # up names for almost every single method
        curl_options = {
            'GET': pycurl.HTTPGET,
            'POST': pycurl.POST,
            'PUT': pycurl.UPLOAD,
            'HEAD': pycurl.NOBODY,
        }
        custom_methods = {'DELETE', 'OPTIONS', 'PATCH', 'TRACE'}
        for o in curl_options.values():
            curl.setopt(o, False)
        if request.method in curl_options:
            curl.unsetopt(pycurl.CUSTOMREQUEST)
            curl.setopt(curl_options[request.method], True)
        elif request.allow_nonstandard_methods or request.method in custom_methods:
            curl.setopt(pycurl.CUSTOMREQUEST, request.method)
        else:
            raise KeyError('unknown method ' + request.method)

        body_expected = request.method in ("POST", "PATCH", "PUT")
        body_present = request.body is not None
        if not request.allow_nonstandard_methods:
            # Some HTTP methods nearly always have bodies while others
            # almost never do. Fail in this case unless the user has
            # opted out of sanity checks with allow_nonstandard_methods.
            if ((body_expected and not body_present) or
                    (body_present and not body_expected)):
                raise ValueError(
                    'Body must %sbe None for method %s (unless '
                    'allow_nonstandard_methods is true)' %
                    ('not ' if body_expected else '', request.method))

        if body_expected or body_present:
            if request.method == "GET":
                # Even with `allow_nonstandard_methods` we disallow
                # GET with a body (because libcurl doesn't allow it
                # unless we use CUSTOMREQUEST). While the spec doesn't
                # forbid clients from sending a body, it arguably
                # disallows the server from doing anything with them.
                raise ValueError('Body must be None for GET request')
            body = (request.body or '').encode()
            request_buffer = io.BytesIO(body)

            def ioctl(cmd):
                if cmd == curl.IOCMD_RESTARTREAD:
                    request_buffer.seek(0)
            curl.setopt(pycurl.READFUNCTION, request_buffer.read)
            curl.setopt(pycurl.IOCTLFUNCTION, ioctl)
            if request.method == 'POST':
                curl.setopt(pycurl.POSTFIELDSIZE, len(body))
            else:
                curl.setopt(pycurl.UPLOAD, True)
                curl.setopt(pycurl.INFILESIZE, len(body))

        if request.auth_username is not None:
            userpwd = '{}:{}'.format(
                request.auth_username, request.auth_password or '')

            if request.auth_mode is None or request.auth_mode == 'basic':
                curl.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_BASIC)
            elif request.auth_mode == 'digest':
                curl.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_DIGEST)
            else:
                raise ValueError(
                    "Unsupported auth_mode {}".format(request.auth_mode))

            curl.setopt(pycurl.USERPWD, userpwd.encode())
            curl_log.debug("%s %s (username: %r)", request.method, request.url,
                           request.auth_username)
        else:
            curl.unsetopt(pycurl.USERPWD)
            # curl_log.debug("%s %s", request.method, request.url)

        if request.client_cert is not None:
            curl.setopt(pycurl.SSLCERT, request.client_cert)

        if request.client_key is not None:
            curl.setopt(pycurl.SSLKEY, request.client_key)

        if request.ssl_options is not None:
            raise ValueError("ssl_options not supported in curl_httpclient")

        if threading.activeCount() > 1:
            # libcurl/pycurl is not thread-safe by default.  When multiple threads
            # are used, signals should be disabled.  This has the side effect
            # of disabling DNS timeouts in some environments (when libcurl is
            # not linked against ares), so we don't do it when there is only one
            # thread.  Applications that use many short-lived threads may need
            # to set NOSIGNAL manually in a prepare_curl_callback since
            # there may not be any other threads running at the time we call
            # threading.activeCount.
            curl_log.debug('threading activeCount is greater than 1.')
            curl.setopt(pycurl.NOSIGNAL, 1)
        if request.prepare_curl_callback is not None:
            request.prepare_curl_callback(curl)

    def _curl_header_callback(self,
                              headers_buf: io.BytesIO,
                              header_callback: typing.Callable,
                              header_line: bytes):
        if header_callback is not None:
            self.loop.call_soon(header_callback, header_line)
        # header_line as returned by curl includes the end-of-line characters.
        # whitespace at the start should be preserved to allow multi-line headers
        header_line = header_line.rstrip()
        if header_line.startswith(b'HTTP/'):
            # clear headers buf
            headers_buf.seek(0)
            headers_buf.truncate()
            try:
                (__, __, reason) = httputil.parse_response_start_line(
                    header_line.decode('latin-1'))
                header_line = "X-Http-Reason: {}".format(reason).encode()
            except httputil.HTTPInputError:
                return
        if not header_line:
            return
        headers_buf.write(header_line)

    def _curl_debug(self, debug_type, debug_msg):
        debug_types = ('I', '<', '>', '<', '>')
        if debug_type == 0:
            curl_log.debug('%s', debug_msg.strip())
        elif debug_type in (1, 2):
            for line in debug_msg.splitlines():
                curl_log.debug('%s %s', debug_types[debug_type], line)
        elif debug_type == 4:
            curl_log.debug('%s %r', debug_types[debug_type], debug_msg)



async def _test():
    def gen_request(url):
        return HTTPRequest(url)

    session = ClientSession()
    reqs = (
        gen_request(url) for url in [
            'http://www.baidu.com/',
            'http://www.163.com/',
            'http://www.sina.com/',
            'http://www.sohu.com/',
            'http://www.hao123.com/',
            'http://www.taobao.com/',
        ]
    )
    result = await asyncio.gather(
        *(session.fetch(req) for req in reqs)
    )
    print('result ------ ', result)

async def test():
    start = time.time()
    # url = 'http://127.0.0.1:8080/index.html'
    url = 'http://127.0.0.1:8080/taobao.html'

    print('start:', start)
    session = ClientSession()
    for response in await asyncio.gather(
            *(session.fetch(HTTPRequest(url))
            for i in range(1000))
        ):
        _ = response.headers

    end = time.time()
    print('end:', end)
    print('cost:', end - start)

async def test_single():
    start = time.time()
    # url = 'http://127.0.0.1:8080/index.html'
    url = 'http://127.0.0.1:8080/taobao.html'

    print('start:', start)
    session = ClientSession()
    result = await session.fetch(HTTPRequest(url))

    end = time.time()
    print('end:', end)
    print('cost:', end - start)

    print('result.status ------ ', result.code)
    # print('headers ---- ', result.headers)
    # print('body ---- ', result.body)


def main():
    loop = asyncio.get_event_loop()
    # loop.run_until_complete(_test())
    loop.run_until_complete(test())
    # loop.run_until_complete(test_single())

if __name__ == '__main__':
    main()
