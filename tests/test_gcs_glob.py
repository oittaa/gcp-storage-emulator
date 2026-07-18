from unittest import TestCase
from unittest.mock import MagicMock

from gcp_storage_emulator.gcs_glob import expand_braces, gcs_glob_match
from gcp_storage_emulator.server import Request


class GcsGlobTests(TestCase):
    def test_expand_braces(self):
        self.assertEqual(
            sorted(expand_braces("foo/{a,b}")),
            ["foo/a", "foo/b"],
        )
        self.assertEqual(
            sorted(expand_braces("**/{foobar,baz}")),
            ["**/baz", "**/foobar"],
        )

    def test_match_glob_examples(self):
        blob_names = ["foo/bar", "foo/baz", "foo/foobar", "foobar"]
        expected = {
            "foo*bar": ["foobar"],
            "foo**bar": ["foo/bar", "foo/foobar", "foobar"],
            "**/foobar": ["foo/foobar", "foobar"],
            "*/ba[rz]": ["foo/bar", "foo/baz"],
            "*/ba[!a-y]": ["foo/baz"],
            "**/{foobar,baz}": ["foo/baz", "foo/foobar", "foobar"],
            "foo/{foo*,*baz}": ["foo/baz", "foo/foobar"],
        }
        for pattern, names in expected.items():
            matched = [name for name in blob_names if gcs_glob_match(pattern, name)]
            self.assertEqual(matched, names, pattern)


class RequestBaseUrlTests(TestCase):
    def test_base_url_prefers_host_header(self):
        handler = MagicMock()
        handler.path = "/upload/storage/v1/b/bucket/o?uploadType=resumable"
        handler.headers = {"Host": "storage.example.test:9023"}
        handler.server.server_address = ("0.0.0.0", 9023)

        request = Request(handler, "POST")
        self.assertEqual(request.base_url, "http://storage.example.test:9023")
        self.assertNotIn("0.0.0.0", request.base_url)

    def test_base_url_falls_back_to_bind_address(self):
        handler = MagicMock()
        handler.path = "/"
        handler.headers = {}
        handler.server.server_address = ("127.0.0.1", 8080)

        request = Request(handler, "GET")
        self.assertEqual(request.base_url, "http://127.0.0.1:8080")
