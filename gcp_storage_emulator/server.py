import gzip
import json
import logging
import re
import threading
import time
import zlib
from email.parser import BytesParser
from functools import partial
from http import HTTPStatus, server
from urllib.parse import parse_qs, unquote, urlparse

from gcp_storage_emulator import settings
from gcp_storage_emulator.handlers import buckets, objects
from gcp_storage_emulator.storage import Storage

logger = logging.getLogger(__name__)

GET = "GET"
POST = "POST"
PUT = "PUT"
DELETE = "DELETE"
PATCH = "PATCH"


def _wipe_data(req, res, storage):
    keep_buckets = bool(req.query.get("keep-buckets"))
    logger.debug("Wiping storage")
    if keep_buckets:
        logger.debug("...while keeping the buckets")
    storage.wipe(keep_buckets)

    logger.debug("Storage wiped")
    res.write("OK")


def _health_check(req, res, storage):
    res.write("OK")


HANDLERS = (
    (r"^{}/b$".format(settings.API_ENDPOINT), {GET: buckets.ls, POST: buckets.insert}),
    (
        r"^{}/b/(?P<bucket_name>[-.\w]+)$".format(settings.API_ENDPOINT),
        {GET: buckets.get, DELETE: buckets.delete},
    ),
    (
        r"^{}/b/(?P<bucket_name>[-.\w]+)/o$".format(settings.API_ENDPOINT),
        {GET: objects.ls},
    ),
    (
        r"^{}/b/(?P<bucket_name>[-.\w]+)/o/(?P<object_id>.*[^/]+)/copyTo/b/".format(
            settings.API_ENDPOINT
        )
        + r"(?P<dest_bucket_name>[-.\w]+)/o/(?P<dest_object_id>.*[^/]+)$",
        {POST: objects.copy},
    ),
    (
        r"^{}/b/(?P<bucket_name>[-.\w]+)/o/(?P<object_id>.*[^/]+)/compose$".format(
            settings.API_ENDPOINT
        ),
        {POST: objects.compose},
    ),
    (
        r"^{}/b/(?P<bucket_name>[-.\w]+)/o/(?P<object_id>.*[^/]+)$".format(
            settings.API_ENDPOINT
        ),
        {GET: objects.get, DELETE: objects.delete, PATCH: objects.patch},
    ),
    # Non-default API endpoints
    (
        r"^{}/b/(?P<bucket_name>[-.\w]+)/o$".format(settings.UPLOAD_API_ENDPOINT),
        {POST: objects.insert, PUT: objects.upload_partial},
    ),
    (
        r"^{}/b/(?P<bucket_name>[-.\w]+)/o/(?P<object_id>.*[^/]+)$".format(
            settings.DOWNLOAD_API_ENDPOINT
        ),
        {GET: objects.download},
    ),
    (
        r"^{}$".format(settings.BATCH_API_ENDPOINT),
        {POST: objects.batch},
    ),
    # Internal API, not supported by the real GCS
    (r"^/$", {GET: _health_check}),  # Health check endpoint
    (r"^/wipe$", {GET: _wipe_data}),  # Wipe all data
    # Public file serving, same as object.download and signed URLs
    (
        r"^/(?P<bucket_name>[-.\w]+)/(?P<object_id>.*[^/]+)$",
        {GET: objects.download, PUT: objects.xml_upload},
    ),
)

BATCH_HANDLERS = (
    r"^(?P<method>[\w]+).*{}/b/(?P<bucket_name>[-.\w]+)/o/(?P<object_id>[^\?]+[^/])([\?].*)?$".format(
        settings.API_ENDPOINT
    ),
    r"^(?P<method>[\w]+).*{}/b/(?P<bucket_name>[-.\w]+)([\?].*)?$".format(
        settings.API_ENDPOINT
    ),
    r"^Content-Type:\s*(?P<content_type>[-.\w/]+)$",
)


def _parse_batch_item(item):
    parsed_params = {}
    content_reached = None
    partial_content = ""
    current_content = item.get_payload()
    for line in current_content.splitlines():
        if not content_reached:
            if not line:
                content_reached = True
            else:
                for regex in BATCH_HANDLERS:
                    pattern = re.compile(regex)
                    match = pattern.fullmatch(line)
                    if match:
                        for k, v in match.groupdict().items():
                            parsed_params[k] = unquote(v)
        else:
            partial_content += line
    if partial_content and parsed_params.get("content_type") == "application/json":
        parsed_params["meta"] = json.loads(partial_content)
    return parsed_params


def _read_raw_data(request_handler):
    if request_handler.headers["Content-Length"]:
        return request_handler.rfile.read(
            int(request_handler.headers["Content-Length"])
        )

    if request_handler.headers["Transfer-Encoding"] == "chunked":
        raw_data = b""

        while True:
            line = request_handler.rfile.readline().strip()
            chunk_size = int(line, 16) if line else 0
            if chunk_size == 0:
                break

            raw_data += request_handler.rfile.read(chunk_size)

            request_handler.rfile.readline()

        return raw_data

    return None


def _decode_raw_data(raw_data, request_handler):
    if not raw_data:
        return None

    if request_handler.headers["Content-Encoding"] == "gzip":
        return gzip.decompress(raw_data)

    if request_handler.headers["Content-Encoding"] == "deflate":
        return zlib.decompress(raw_data)

    return raw_data


def _read_data(request_handler):
    raw_data = _decode_raw_data(_read_raw_data(request_handler), request_handler)

    if not raw_data:
        return None

    content_type = request_handler.headers["Content-Type"] or "application/octet-stream"

    if content_type.startswith("application/json"):
        return json.loads(raw_data)

    if content_type.startswith("multipart/"):
        parser = BytesParser()
        header = bytes("Content-Type:" + content_type + "\r\n", "utf-8")

        msg = parser.parsebytes(header + raw_data)
        payload = msg.get_payload()

        if content_type.startswith("multipart/mixed"):
            # Batch https://cloud.google.com/storage/docs/json_api/v1/how-tos/batch
            rv = list()
            for item in payload:
                parsed_params = _parse_batch_item(item)
                rv.append(parsed_params)

            return rv

        # For multipart upload, google API expect the first item to be a json-encoded
        # object, and the second (and only other) part, the file content
        return {
            "meta": json.loads(payload[0].get_payload()),
            "content": payload[1].get_payload(decode=True),
            "content-type": payload[1].get_content_type(),
        }

    return raw_data


class Request(object):
    def __init__(self, request_handler, method):
        super().__init__()
        self._path = request_handler.path
        self._request_handler = request_handler
        self._server_address = request_handler.server.server_address
        self._base_url = "http://{}:{}".format(
            self._server_address[0], self._server_address[1]
        )
        self._full_url = self._base_url + self._path
        self._parsed_url = urlparse(self._full_url)
        self._query = parse_qs(self._parsed_url.query)
        self._methtod = method
        self._data = None
        self._parsed_params = None

    @property
    def path(self):
        return self._parsed_url.path

    @property
    def base_url(self):
        return self._base_url

    @property
    def full_url(self):
        return self._full_url

    @property
    def method(self):
        return self._methtod

    @property
    def query(self):
        return self._query

    @property
    def params(self):
        if not self._match:
            return None

        if not self._parsed_params:
            self._parsed_params = {}
            for k, v in self._match.groupdict().items():
                self._parsed_params[k] = unquote(v)
        return self._parsed_params

    @property
    def data(self):
        if not self._data:
            self._data = _read_data(self._request_handler)
        return self._data

    def get_header(self, key, default=None):
        return self._request_handler.headers.get(key, default)

    def set_match(self, match):
        self._match = match


class Response(object):
    def __init__(self, handler):
        super().__init__()
        self._handler = handler
        self.status = HTTPStatus.OK
        self._headers = {}
        self._content = ""

    def write(self, content):
        logger.warning(
            "[RESPONSE] Content handled as string, should be handled as stream"
        )
        self._content += content

    def write_file(self, content, content_type="application/octet-stream"):
        if content_type is not None:
            self["Content-type"] = content_type

        self._content = content

    def json(self, obj):
        self["Content-type"] = "application/json"
        self._content = json.dumps(obj)

    def __setitem__(self, key, value):
        self._headers[key] = value

    def __getitem__(self, key):
        return self._headers[key]

    def close(self):
        self._handler.send_response(self.status.value, self.status.phrase)
        for (k, v) in self._headers.items():
            self._handler.send_header(k, v)

        content = self._content

        if isinstance(self._content, str):
            content = self._content.encode("utf-8")

        self._handler.send_header("Content-Length", str(len(content)))
        self._handler.end_headers()
        self._handler.wfile.write(content)


class Router(object):
    def __init__(self, request_handler):
        super().__init__()
        self._request_handler = request_handler

    def handle(self, method):
        if self._request_handler.headers["x-http-method-override"]:
            method = self._request_handler.headers["x-http-method-override"]

        request = Request(self._request_handler, method)
        response = Response(self._request_handler)

        for regex, handlers in HANDLERS:
            pattern = re.compile(regex)
            match = pattern.fullmatch(request.path)
            if match:
                request.set_match(match)
                handler = handlers.get(method)
                try:
                    handler(request, response, self._request_handler.storage)
                except Exception as e:
                    logger.error(
                        "An error has occurred while running the handler for {} {}".format(
                            request.method,
                            request.full_url,
                        )
                    )
                    logger.error(e)
                    raise e
                break
        else:
            logger.error(
                "Method not implemented: {} - {}".format(request.method, request.path)
            )
            response.status = HTTPStatus.NOT_IMPLEMENTED

        response.close()


class RequestHandler(server.BaseHTTPRequestHandler):
    def __init__(self, storage, *args, **kwargs):
        self.storage = storage
        super().__init__(*args, **kwargs)

    def do_GET(self):
        router = Router(self)
        router.handle(GET)

    def do_POST(self):
        router = Router(self)
        router.handle(POST)

    def do_DELETE(self):
        router = Router(self)
        router.handle(DELETE)

    def do_PUT(self):
        router = Router(self)
        router.handle(PUT)

    def do_PATCH(self):
        router = Router(self)
        router.handle(PATCH)

    def log_message(self, format, *args):
        logger.info(format % args)


class APIThread(threading.Thread):
    def __init__(self, host, port, storage, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._host = host
        self._port = port
        self.is_running = threading.Event()
        self._httpd = None
        self._storage = storage

    def run(self):
        self._httpd = server.HTTPServer(
            (self._host, self._port), partial(RequestHandler, self._storage)
        )
        self.is_running.set()
        self._httpd.serve_forever()

    def join(self, timeout=None):
        self.is_running.clear()
        if self._httpd:
            logger.info("[API] Stopping API server")
            self._httpd.shutdown()
            self._httpd.server_close()


class Server(object):
    def __init__(self, host, port, in_memory, default_bucket=None, data_dir=None):
        self._storage = Storage(use_memory_fs=in_memory, data_dir=data_dir)
        if default_bucket:
            logger.debug('[SERVER] Creating default bucket "{}"'.format(default_bucket))
            buckets.create_bucket(default_bucket, self._storage)
        self._api = APIThread(host, port, self._storage)

    def start(self):
        self._api.start()
        self._api.is_running.wait()  # Start the API thread

    def stop(self):
        self._api.join(timeout=1)

    def wipe(self, keep_buckets=False):
        self._storage.wipe(keep_buckets=keep_buckets)

    def run(self):
        try:
            self.start()
            logger.info("[SERVER] All services started")

            while True:
                try:
                    time.sleep(0.1)
                except KeyboardInterrupt:
                    logger.info("[SERVER] Received keyboard interrupt")
                    break

        finally:
            self.stop()


def create_server(host, port, in_memory=False, default_bucket=None, data_dir=None):
    logger.info("Starting server at {}:{}".format(host, port))
    return Server(
        host,
        port,
        in_memory=in_memory,
        default_bucket=default_bucket,
        data_dir=data_dir,
    )
