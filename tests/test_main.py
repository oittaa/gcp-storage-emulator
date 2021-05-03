import os
from unittest import TestCase as BaseTestCase

import requests

from gcp_storage_emulator.__main__ import main, wipe


def _get_storage_client(http):
    """Gets a python storage client"""
    os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:9099"

    # Cloud storage uses environment variables to configure api endpoints for
    # file upload - which is read at module import time
    from google.cloud import storage

    if os.getenv("DEBUG"):
        from http import client as http_client

        http_client.HTTPConnection.debuglevel = 5
    return storage.Client(
        project="[PROJECT]",
        _http=http,
        client_options={"api_endpoint": "http://localhost:9099"},
    )


class ServerBaseCase(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls._server = main(["start", "--port=9099"], True)
        cls._server.start()

    @classmethod
    def tearDownClass(cls):
        wipe()
        cls._server.stop()

    def setUp(self):
        self._session = requests.Session()
        self._client = _get_storage_client(self._session)


class MainHttpEndpointsTest(ServerBaseCase):
    """Tests for the HTTP endpoints."""

    def _url(self, path):
        return os.environ["STORAGE_EMULATOR_HOST"] + path

    def test_health_check(self):
        url = self._url("/")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, "OK".encode("utf-8"))

    def test_wipe(self):
        url = self._url("/wipe")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, "OK".encode("utf-8"))

    def test_download_by_url(self):
        """Objects should be downloadable over HTTP from the emulator client."""
        content = "Here is some content"
        bucket = self._client.create_bucket("anotherbucket")
        blob = bucket.blob("something.txt")
        blob.upload_from_string(content)

        url = self._url("/anotherbucket/something.txt")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, content.encode("utf-8"))

    def test_path_does_not_exist(self):
        url = self._url("/zzzzz-does-not-exist")
        response = requests.get(url)
        self.assertEqual(response.status_code, 501)
        self.assertEqual(response.content, "".encode("utf-8"))
