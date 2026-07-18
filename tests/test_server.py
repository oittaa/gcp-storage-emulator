import datetime
import json
import os
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import TestCase as BaseTestCase

import requests
from google.api_core.exceptions import BadRequest, Conflict, NotFound
from google.auth.credentials import AnonymousCredentials, Signing

from gcp_storage_emulator.server import create_server
from gcp_storage_emulator.settings import STORAGE_BASE, STORAGE_DIR

TEST_TEXT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_text.txt")


def _storage_path(*parts: str) -> Path:
    return Path(STORAGE_BASE, STORAGE_DIR, *parts)


class FakeSigningCredentials(Signing, AnonymousCredentials):
    def sign_bytes(self, message):
        return b"foobar"

    @property
    def signer_email(self):
        return "foobar@example.tld"

    @property
    def signer(self):
        pass


def _get_storage_client(http):
    """Gets a python storage client"""
    os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:9023"

    # Cloud Storage uses environment variables to configure API endpoints for
    # file upload - which is read at module import time
    from google.cloud import storage

    if os.getenv("DEBUG"):
        from http import client as http_client

        http_client.HTTPConnection.debuglevel = 5
    return storage.Client(
        project="[PROJECT]",
        _http=http,
        client_options={"api_endpoint": "http://localhost:9023"},
    )


class ServerBaseCase(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls._server = create_server("localhost", 9023, in_memory=False)
        cls._server.start()

    @classmethod
    def tearDownClass(cls):
        cls._server.wipe()
        cls._server.stop()

    def setUp(self):
        self._session = requests.Session()
        self._client = _get_storage_client(self._session)
        self._server.wipe()


class BucketsTests(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        cls._server = create_server("localhost", 9023, in_memory=False)
        cls._server.start()

    @classmethod
    def tearDownClass(cls):
        cls._server.wipe()
        cls._server.stop()

    def setUp(self):
        self._server.wipe()
        self._session = requests.Session()
        self._client = _get_storage_client(self._session)

    def test_bucket_creation(self):
        bucket = self._client.create_bucket("bucket_name")
        self.assertEqual(bucket.project_number, 1234)

    def test_bucket_creation_no_override(self):
        self._client.create_bucket("bucket_name")
        with self.assertRaises(Conflict):
            self._client.create_bucket("bucket_name")

    def test_bucket_list(self):
        bucket = self._client.create_bucket("bucket_name")
        all_bucket_names = [b.name for b in self._client.list_buckets()]
        self.assertIn(bucket.name, all_bucket_names)

    def test_bucket_get_existing(self):
        bucket = self._client.create_bucket("bucket_name")
        fetched_bucket = self._client.get_bucket("bucket_name")
        self.assertEqual(fetched_bucket.name, bucket.name)

    def test_bucket_get_existing_with_dot(self):
        bucket = self._client.create_bucket("bucket.name")
        fetched_bucket = self._client.get_bucket("bucket.name")
        self.assertEqual(fetched_bucket.name, bucket.name)

    def test_bucket_get_non_existing(self):
        with self.assertRaises(NotFound):
            self._client.get_bucket("bucket_name")

    def test_bucket_delete(self):
        bucket = self._client.create_bucket("bucket_name")
        bucket.delete()

        with self.assertRaises(NotFound):
            self._client.get_bucket("bucket_name")

    def test_bucket_delete_removes_file(self):
        bucket = self._client.create_bucket("bucket_name")
        bucket.delete()

        self.assertFalse(_storage_path("bucket_name").exists())

    def test_bucket_delete_non_existing(self):
        # client.bucket doesn't create the actual bucket resource remotely
        bucket = self._client.bucket("bucket_name")
        with self.assertRaises(NotFound):
            bucket.delete()

    def test_bucket_delete_non_empty(self):
        bucket = self._client.create_bucket("bucket_name")
        blob = bucket.blob("canttouchme.txt")
        blob.upload_from_string("This should prevent deletion if not force")

        with self.assertRaises(Conflict):
            bucket.delete()

        blob = bucket.get_blob("canttouchme.txt")
        self.assertIsNotNone(blob)

    def test_bucket_force_delete(self):
        bucket = self._client.create_bucket("bucket_name")
        blob = bucket.blob("cantouchme.txt")
        blob.upload_from_string("This should prevent deletion if not force")

        bucket.delete(force=True)

        blob = bucket.get_blob("cantouchme.txt")
        self.assertIsNone(blob)

        self.assertFalse(_storage_path("bucket_name").exists())


class DefaultBucketTests(BaseTestCase):
    def tearDown(self):
        if self._server:
            self._server.wipe()
            self._server.stop()
        return super().tearDown()

    def test_bucket_created(self):
        self._server = create_server(
            "localhost", 9023, in_memory=True, default_bucket="example.appspot.com"
        )
        self._server.start()
        self._session = requests.Session()
        self._client = _get_storage_client(self._session)
        bucket = self._client.get_bucket("example.appspot.com")
        self.assertEqual(bucket.name, "example.appspot.com")
        self.assertEqual(bucket.storage_class, "STANDARD")


class ObjectsTests(ServerBaseCase):
    def test_upload_from_string(self):
        content = "this is the content of the file\n"
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("testblob-name.txt")
        blob.upload_from_string(content)

        read_content = _storage_path("testbucket", "testblob-name.txt").read_text(
            encoding="utf-8"
        )
        self.assertEqual(read_content, content)

    def test_upload_from_text_file(self):
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("test_text.txt")
        with open(TEST_TEXT, "rb") as file:
            blob.upload_from_file(file)

        read_content = _storage_path("testbucket", "test_text.txt").read_text(
            encoding="utf-8"
        )

        with open(TEST_TEXT, "rb") as file:
            expected_content = str(file.read(), encoding="utf-8")
            self.assertEqual(read_content, expected_content)

    def test_upload_from_bin_file(self):
        test_binary = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test_binary.png"
        )
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("binary.png")
        with open(test_binary, "rb") as file:
            blob.upload_from_file(file)

        read_content = _storage_path("testbucket", "binary.png").read_bytes()

        with open(test_binary, "rb") as file:
            expected_content = file.read()
            self.assertEqual(read_content, expected_content)

    def test_upload_from_bin_file_cr_lf(self):
        content = b"\r\rheeeeei\r\n"
        test_binary = BytesIO(content)
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("binary_cr.png")

        blob.upload_from_file(test_binary, size=len(content))

        read_content = _storage_path("testbucket", "binary_cr.png").read_bytes()

        self.assertEqual(read_content, content)

    def test_upload_from_file_name(self):
        test_binary = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test_binary.png"
        )
        file_name = "test_binary.png"

        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob(file_name)
        blob.upload_from_filename(test_binary)
        blob = bucket.get_blob(file_name)
        with NamedTemporaryFile() as temp_file:
            blob.download_to_filename(temp_file.name)
            with open(test_binary, "rb") as orig_file:
                self.assertEqual(temp_file.read(), orig_file.read())

    def test_upload_from_file(self):
        test_binary = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test_binary.png"
        )
        file_name = "test_binary.png"

        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob(file_name)
        with open(test_binary, "rb") as filehandle:
            blob.upload_from_file(filehandle)
            self.assertTrue(blob.id.startswith("testbucket/test_binary.png/"))

    def test_get(self):
        file_name = "testblob-name.txt"
        content = "this is the content of the file\n"
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob(file_name)
        blob.upload_from_string(content)

        blob = bucket.get_blob(file_name)
        self.assertEqual(blob.name, file_name)

    def test_get_unicode(self):
        file_name = "tmp.ąćęłńóśźż.马铃薯.zip"
        content = "this is the content of the file\n"
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob(file_name)
        blob.upload_from_string(content)

        blob = bucket.get_blob(file_name)
        self.assertEqual(blob.name, file_name)

    def test_get_nonexistent(self):
        bucket = self._client.create_bucket("testbucket")
        res = bucket.get_blob("idonotexist")

        self.assertIsNone(res)

        blob = bucket.blob("iexist")
        blob.upload_from_string("some_fake_content")
        res = bucket.get_blob("idonotexist")

        self.assertIsNone(res)

    def test_download_nonexistent(self):
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("idonotexist")
        with self.assertRaises(NotFound):
            blob.download_as_bytes()

    def test_upload_to_nonexistent_bucket(self):
        bucket = self._client.bucket("non-existent-test-bucket")
        blob = bucket.blob("idonotexisteither")
        with self.assertRaises(NotFound):
            blob.upload_from_string("some_content")

    def test_download_as_bytes(self):
        content = "The quick brown fox jumps over the lazy dog\n"
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("iexist")
        blob.upload_from_string(content)

        blob = bucket.get_blob("iexist")
        fetched_content = blob.download_as_bytes()
        self.assertEqual(fetched_content, content.encode("utf-8"))

    def test_download_range_start(self):
        content = b"123456789"
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("iexist")
        blob.upload_from_string(content)

        blob = bucket.get_blob("iexist")
        fetched_content = blob.download_as_bytes(start=2)
        self.assertEqual(fetched_content, b"3456789")

    def test_download_range_end(self):
        content = b"123456789"
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("iexist")
        blob.upload_from_string(content)

        blob = bucket.get_blob("iexist")
        fetched_content = blob.download_as_bytes(end=4)
        self.assertEqual(fetched_content, b"12345")

    def test_download_range_start_end(self):
        content = b"123456789"
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("iexist")
        blob.upload_from_string(content)

        blob = bucket.get_blob("iexist")
        fetched_content = blob.download_as_bytes(start=2, end=4)
        self.assertEqual(fetched_content, b"345")

    def test_set_content_encoding(self):
        content = "The quick brown fox jumps over the lazy dog\n"
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("testblob")
        blob.content_encoding = "gzip"
        blob.upload_from_string(content)
        blob.reload()
        self.assertEqual(blob.content_encoding, "gzip")

    def test_set_metadata(self):
        content = "The quick brown fox jumps over the lazy dog\n"
        bucket = self._client.create_bucket("testbucket")
        metadata = {"Color": "Pink"}

        blob = bucket.blob("testblob")
        blob.metadata = metadata
        blob.upload_from_string(content)
        blob.reload()
        self.assertEqual(blob.metadata, metadata)

    def test_download_includes_x_goog_meta_headers(self):
        """Custom metadata is returned as x-goog-meta-* on object download (#187)."""
        content = b"helloworld"
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("meta-object")
        blob.metadata = {"some": "metadata", "Color": "Pink"}
        blob.upload_from_string(content)

        # JSON API media download path
        url = (
            "http://localhost:9023/download/storage/v1/b/testbucket/o/"
            "meta-object?alt=media"
        )
        response = self._session.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, content)
        # HTTP headers are case-insensitive; requests normalizes to lowercase keys.
        self.assertEqual(response.headers.get("x-goog-meta-some"), "metadata")
        self.assertEqual(response.headers.get("x-goog-meta-color"), "Pink")

    def test_public_path_download_includes_x_goog_meta_headers(self):
        content = b"public-meta"
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("public-meta.txt")
        blob.metadata = {"reviewer": "jane"}
        blob.upload_from_string(content)

        # Public / signed-URL style path: /{bucket}/{object}
        response = self._session.get("http://localhost:9023/testbucket/public-meta.txt")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, content)
        self.assertEqual(response.headers.get("x-goog-meta-reviewer"), "jane")

    def test_download_without_custom_metadata_omits_x_goog_meta_headers(self):
        content = b"no-meta"
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("plain.txt")
        blob.upload_from_string(content)

        response = self._session.get(
            "http://localhost:9023/download/storage/v1/b/testbucket/o/plain.txt?alt=media"
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(
            any(name.lower().startswith("x-goog-meta-") for name in response.headers)
        )

    def test_set_custom_time(self):
        content = "The quick brown fox jumps over the lazy dog\n"
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("customtime")
        now = datetime.datetime.now(datetime.timezone.utc)
        blob.custom_time = now
        blob.upload_from_string(content)
        blob.reload()
        self.assertEqual(blob.custom_time, now)

    def test_patch_custom_time(self):
        content = "The quick brown fox jumps over the lazy dog\n"
        now = datetime.datetime.now(datetime.timezone.utc)
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("customtime")
        blob.upload_from_string(content)

        blob.reload()
        self.assertEqual(blob.custom_time, None)
        blob.custom_time = now
        blob.patch()
        blob.reload()
        self.assertEqual(blob.custom_time, now)

    def test_patch_custom_time_with_older_datetime(self):
        content = "The quick brown fox jumps over the lazy dog\n"
        newer = datetime.datetime.now(datetime.timezone.utc)
        older = datetime.datetime(2014, 11, 5, 20, 34, 37)
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("customtime")
        blob.upload_from_string(content)

        blob.reload()
        self.assertEqual(blob.custom_time, None)
        blob.custom_time = newer
        blob.patch()
        blob.reload()
        self.assertEqual(blob.custom_time, newer)
        blob.custom_time = older
        blob.patch()
        blob.reload()
        self.assertEqual(blob.custom_time, newer)

    def test_patch_content_encoding(self):
        content = "The quick brown fox jumps over the lazy dog\n"
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("testblob")
        blob.content_encoding = "gzip"
        blob.upload_from_string(content)
        blob.reload()
        metageneration = blob.metageneration
        self.assertEqual(blob.content_encoding, "gzip")
        blob.content_encoding = ""
        blob.patch()
        blob.reload()
        self.assertNotEqual(blob.metageneration, metageneration)
        self.assertEqual(blob.content_encoding, "")

    def test_valid_md5_hash(self):
        content = b"test"
        md5_hash = "CY9rzUYh03PK3k6DJie09g=="
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("hashtest")
        blob.md5_hash = md5_hash
        blob.upload_from_string(content)
        download_blob = bucket.get_blob("hashtest")
        self.assertEqual(download_blob.download_as_bytes(checksum="md5"), content)
        self.assertEqual(download_blob.md5_hash, md5_hash)

    def test_invalid_md5_hash(self):
        content = b"Hello World"
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("hashtest")
        blob.md5_hash = "deadbeef"
        with self.assertRaises(BadRequest):
            blob.upload_from_string(content)

    def test_valid_crc32c_hash(self):
        content = b"hello world"
        crc32c_hash = "yZRlqg=="
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("hashtest")
        blob.crc32c = crc32c_hash
        blob.upload_from_string(content)
        download_blob = bucket.get_blob("hashtest")
        self.assertEqual(download_blob.download_as_bytes(checksum="crc32c"), content)
        self.assertEqual(download_blob.crc32c, crc32c_hash)

    def test_invalid_crc32c_hash(self):
        # google-cloud-storage 3.x no longer forwards a pre-set invalid crc32c on
        # upload_from_string; hit the multipart API directly so the server still
        # validates checksums independently of client library behaviour.
        content = b"Hello World"
        self._client.create_bucket("testbucket")
        boundary = "===============boundary=="
        metadata = json.dumps({"name": "hashtest", "crc32c": "deadbeef"})
        body = (
            (
                f"--{boundary}\r\n"
                "Content-Type: application/json; charset=UTF-8\r\n\r\n"
                f"{metadata}\r\n"
                f"--{boundary}\r\n"
                "Content-Type: application/octet-stream\r\n\r\n"
            ).encode("utf-8")
            + content
            + f"\r\n--{boundary}--\r\n".encode("utf-8")
        )
        response = self._session.post(
            "http://localhost:9023/upload/storage/v1/b/testbucket/o?uploadType=multipart",
            data=body,
            headers={"Content-Type": f"multipart/related; boundary={boundary}"},
        )
        self.assertEqual(response.status_code, 400)

    def test_download_binary_to_file(self):
        test_binary = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test_binary.png"
        )
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("binary.png")
        with open(test_binary, "rb") as file:
            blob.upload_from_file(file, content_type="image/png")

        blob = bucket.get_blob("binary.png")
        fetched_file = BytesIO()
        blob.download_to_file(fetched_file)

        with open(test_binary, "rb") as file:
            self.assertEqual(fetched_file.getvalue(), file.read())

    def test_download_text_to_file(self):
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("text.txt")
        with open(TEST_TEXT, "rb") as file:
            blob.upload_from_file(file, content_type="text/plain; charset=utf-8")

        blob = bucket.get_blob("text.txt")
        fetched_file = BytesIO()
        blob.download_to_file(fetched_file)

        with open(TEST_TEXT, "rb") as file:
            self.assertEqual(fetched_file.getvalue(), file.read())

    def test_delete_object(self):
        bucket = self._client.create_bucket("bucket_name")
        blob = bucket.blob("canttouchme.txt")
        blob.upload_from_string("File content")

        path = _storage_path("bucket_name", "canttouchme.txt")
        self.assertTrue(path.exists())
        blob.delete()

        self.assertIsNone(bucket.get_blob("cantouchme.txt"))
        self.assertFalse(path.exists())

    def test_delete_nonexistent_object(self):
        bucket = self._client.create_bucket("bucket_name")
        blob = bucket.blob("this-should-not-exists.txt")

        with self.assertRaises(NotFound):
            blob.delete()

    def test_create_within_directory(self):
        bucket = self._client.create_bucket("bucket_name")
        blob = bucket.blob("this/is/a/nested/file.txt")
        blob.upload_from_string("Not even joking!")

        read_content = _storage_path(
            "bucket_name", "this", "is", "a", "nested", "file.txt"
        ).read_text(encoding="utf-8")
        self.assertEqual(read_content, "Not even joking!")

    def test_create_within_multiple_time_does_not_break(self):
        bucket = self._client.create_bucket("bucket_name")
        blob = bucket.blob("this/is/a/nested/file.txt")
        blob.upload_from_string("Not even joking!")

        bucket.blob("this/is/another/nested/file.txt")
        blob.upload_from_string("Yet another one")

        self.assertTrue(
            _storage_path(
                "bucket_name", "this", "is", "a", "nested", "file.txt"
            ).exists()
        )

    def _assert_blob_list(self, expected, actual):
        self.assertEqual([b.name for b in expected], [b.name for b in actual])

    def test_list_blobs_on_nonexistent_bucket(self):
        blobs = self._client.list_blobs("bucket_name")
        with self.assertRaises(NotFound):
            list(blobs)

    def test_list_blobs_on_empty_bucket(self):
        bucket = self._client.create_bucket("bucket_name")
        blobs = self._client.list_blobs(bucket)
        self._assert_blob_list(blobs, [])

    def test_list_blobs_on_entire_bucket(self):
        bucket_1 = self._client.create_bucket("bucket_name_1")
        bucket_2 = self._client.create_bucket("bucket_name_2")

        blob_1 = bucket_1.blob("a/b.txt")
        blob_1.upload_from_string("text")

        blob_2 = bucket_1.blob("c/d.txt")
        blob_2.upload_from_string("text")

        blob_3 = bucket_2.blob("a/b.txt")
        blob_3.upload_from_string("text")

        blobs = self._client.list_blobs(bucket_1)
        self._assert_blob_list(blobs, [blob_1, blob_2])

    def test_list_blobs_with_prefix(self):
        bucket = self._client.create_bucket("bucket_name")

        blob_1 = bucket.blob("a/b.txt")
        blob_1.upload_from_string("text")

        blob_2 = bucket.blob("a/b/c.txt")
        blob_2.upload_from_string("text")

        blob_3 = bucket.blob("b/c.txt")
        blob_3.upload_from_string("text")

        blobs = self._client.list_blobs(bucket, prefix="a")

        self._assert_blob_list(blobs, [blob_1, blob_2])

    def test_list_blobs_with_match_glob(self):
        bucket = self._client.create_bucket("bucket_name")
        for name in ["foo/bar", "foo/baz", "foo/foobar", "foobar"]:
            bucket.blob(name).upload_from_string("helloworld")

        expected = {
            "foo*bar": ["foobar"],
            "foo**bar": ["foo/bar", "foo/foobar", "foobar"],
            "**/foobar": ["foo/foobar", "foobar"],
            "*/ba[rz]": ["foo/bar", "foo/baz"],
            "*/ba[!a-y]": ["foo/baz"],
            "**/{foobar,baz}": ["foo/baz", "foo/foobar", "foobar"],
            "foo/{foo*,*baz}": ["foo/baz", "foo/foobar"],
        }
        for match_glob, names in expected.items():
            blobs = list(self._client.list_blobs(bucket, match_glob=match_glob))
            self.assertEqual([blob.name for blob in blobs], names, match_glob)

    def test_list_blobs_with_match_glob_and_delimiter(self):
        bucket = self._client.create_bucket("bucket_name")
        for name in ["all/foo/bar", "foo/baz", "foo/389_bar", "bar", "baz"]:
            bucket.blob(name).upload_from_string("helloworld")

        expected = {
            "foo*bar": [],
            "**/bar": ["bar"],
            "**ba[rz]": ["bar", "baz"],
            "*ba[!a-y]": ["baz"],
            "**/{foobar,baz}": ["baz"],
            "*{foo*,*baz}": ["baz"],
        }
        for match_glob, names in expected.items():
            blobs = list(
                self._client.list_blobs(bucket, match_glob=match_glob, delimiter="/")
            )
            self.assertEqual([blob.name for blob in blobs], names, match_glob)

    def test_list_blobs_match_glob_wrong_delimiter(self):
        bucket = self._client.create_bucket("bucket_name")
        with self.assertRaises(BadRequest):
            list(self._client.list_blobs(bucket, delimiter="*", match_glob="*.pdf"))

    def test_list_blobs_with_prefix_and_delimiter(self):
        bucket = self._client.create_bucket("bucket_name")

        blob_1 = bucket.blob("a/b.txt")
        blob_1.upload_from_string("text")

        blob_2 = bucket.blob("a/c.txt")
        blob_2.upload_from_string("text")

        blob_3 = bucket.blob("a/b/c.txt")
        blob_3.upload_from_string("text")

        blob_4 = bucket.blob("b/c.txt")
        blob_4.upload_from_string("text")

        blobs = self._client.list_blobs(bucket, prefix="a/", delimiter="/")

        self._assert_blob_list(blobs, [blob_1, blob_2])
        self.assertEqual(blobs.prefixes, {"a/b/"})

    def test_bucket_copy_existing(self):
        bucket = self._client.create_bucket("bucket_name")

        blob_1 = bucket.blob("a/b.txt")
        blob_1.upload_from_string("text")

        blob_2 = bucket.rename_blob(blob_1, "c/d.txt")

        blobs = self._client.list_blobs(bucket)
        self._assert_blob_list(blobs, [blob_2])

    def test_bucket_copy_non_existing(self):
        bucket = self._client.create_bucket("bucket_name")

        blob_1 = bucket.blob("a/b.txt")

        with self.assertRaises(NotFound):
            bucket.rename_blob(blob_1, "c/d.txt")

    def test_blob_rewrite_response_shape(self):
        """Python client requires totalBytesRewritten/objectSize (issue #305)."""
        content = b"rewrite-me-please"
        bucket = self._client.create_bucket("rewrite_bucket")
        source = bucket.blob("src/file.txt")
        source.upload_from_string(content)

        dest = bucket.blob("dst/file.txt")
        token, bytes_rewritten, total_bytes = dest.rewrite(source)

        self.assertIsNone(token)
        self.assertEqual(bytes_rewritten, len(content))
        self.assertEqual(total_bytes, len(content))
        self.assertEqual(dest.download_as_bytes(), content)
        self.assertEqual(dest.name, "dst/file.txt")

    def test_blob_rewrite_http_response_schema(self):
        """Raw JSON matches official storage#rewriteResponse fields."""
        content = b"schema-check"
        bucket = self._client.create_bucket("rewrite_http")
        source = bucket.blob("a.txt")
        source.upload_from_string(content)

        url = (
            "http://localhost:9023/storage/v1/b/rewrite_http/o/a.txt/"
            "rewriteTo/b/rewrite_http/o/b.txt"
        )
        response = self._session.post(url)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["kind"], "storage#rewriteResponse")
        self.assertTrue(data["done"])
        self.assertEqual(data["totalBytesRewritten"], str(len(content)))
        self.assertEqual(data["objectSize"], str(len(content)))
        self.assertNotIn("written", data)
        self.assertNotIn("size", data)
        self.assertEqual(data["resource"]["name"], "b.txt")
        self.assertEqual(data["resource"]["bucket"], "rewrite_http")

    def test_compose_create_new_blob(self):
        bucket = self._client.create_bucket("compose_test")
        data_1 = b"AAA\n"
        source_1 = bucket.blob("source-1")
        source_1.upload_from_string(data_1, content_type="text/plain")

        data_2 = b"BBB\n"
        source_2 = bucket.blob("source-2")
        source_2.upload_from_string(data_2, content_type="text/plain")

        destination = bucket.blob("destination")
        destination.content_type = "text/somethingelse"
        destination.compose([source_1, source_2])

        composed = destination.download_as_bytes()
        self.assertEqual(composed, data_1 + data_2)
        self.assertEqual(destination.content_type, "text/somethingelse")

    def test_compose_wo_content_type_set(self):
        bucket = self._client.create_bucket("compose_test")
        data_1 = b"AAA\n"
        source_1 = bucket.blob("source-1")
        source_1.upload_from_string(data_1, content_type="text/plain")

        data_2 = b"BBB\n"
        source_2 = bucket.blob("source-2")
        source_2.upload_from_string(data_2, content_type="text/plain")

        destination = bucket.blob("destination")
        destination.compose([source_1, source_2])

        composed = destination.download_as_bytes()
        self.assertEqual(composed, data_1 + data_2)
        self.assertEqual(destination.content_type, "text/plain")

    def test_compose_nonexistent(self):
        bucket = self._client.create_bucket("compose_test")
        source_1 = bucket.blob("source-1")
        source_2 = bucket.blob("source-2")

        destination = bucket.blob("destination")
        with self.assertRaises(NotFound):
            destination.compose([source_1, source_2])

    def test_batch_delete_one(self):
        content = "this is the content of the file\n"
        bucket = self._client.create_bucket("batchbucket")
        blob = bucket.blob("testblob-name1.txt")
        blob.upload_from_string(content)
        with self._client.batch():
            bucket.delete_blob("testblob-name1.txt")
        self.assertIsNone(bucket.get_blob("testblob-name1.txt"))

    def test_batch_delete_nonexistent_blob(self):
        bucket = self._client.create_bucket("batchbucket")
        with self.assertRaises(NotFound):
            with self._client.batch():
                bucket.delete_blob("does-not-exist.txt")

    def test_batch_patch_one(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        content = "this is the content of the file\n"
        bucket = self._client.create_bucket("batchbucket")
        blob = bucket.blob("testblob-name1.txt")
        blob.upload_from_string(content)
        blob.reload()
        self.assertEqual(blob.custom_time, None)
        blob.custom_time = now
        with self._client.batch():
            blob.patch()
        blob = bucket.get_blob("testblob-name1.txt")
        self.assertEqual(blob.custom_time, now)

    def test_batch_delete_two(self):
        content = "this is the content of the file\n"
        bucket = self._client.create_bucket("batchbucket")
        blob = bucket.blob("testblob-name1.txt")
        blob.upload_from_string(content)
        blob = bucket.blob("testblob-name2.txt")
        blob.upload_from_string(content)
        with self._client.batch():
            bucket.delete_blob("testblob-name1.txt")
            bucket.delete_blob("testblob-name2.txt")
        self.assertIsNone(bucket.get_blob("testblob-name1.txt"))
        self.assertIsNone(bucket.get_blob("testblob-name2.txt"))

    def test_batch_patch_two(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        content = "this is the content of the file\n"
        bucket = self._client.create_bucket("batchbucket")
        blob1 = bucket.blob("testblob-name1.txt")
        blob1.upload_from_string(content)
        blob2 = bucket.blob("testblob-name2.txt")
        blob2.upload_from_string(content)
        blob1.reload()
        blob2.reload()
        self.assertEqual(blob1.custom_time, None)
        self.assertEqual(blob2.custom_time, None)
        blob1.custom_time = now
        blob2.custom_time = now
        with self._client.batch():
            blob1.patch()
            blob2.patch()
        blob1 = bucket.get_blob("testblob-name1.txt")
        blob2 = bucket.get_blob("testblob-name2.txt")
        self.assertEqual(blob1.custom_time, now)
        self.assertEqual(blob2.custom_time, now)

    def test_batch_delete_patch(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        content = "this is the content of the file\n"
        bucket = self._client.create_bucket("batchbucket")
        blob = bucket.blob("testblob-name1.txt")
        blob.upload_from_string(content)
        blob = bucket.blob("testblob-name2.txt")
        blob.upload_from_string(content)
        blob = bucket.blob("testblob-name3.txt")
        blob.upload_from_string(content)
        self.assertEqual(blob.custom_time, None)
        blob.custom_time = now
        with self._client.batch():
            bucket.delete_blob("testblob-name1.txt")
            bucket.delete_blob("testblob-name2.txt")
            blob.patch()
        self.assertIsNone(bucket.get_blob("testblob-name1.txt"))
        self.assertIsNone(bucket.get_blob("testblob-name2.txt"))
        blob = bucket.get_blob("testblob-name3.txt")
        self.assertEqual(blob.custom_time, now)

    def test_batch_delete_buckets(self):
        bucket1 = self._client.create_bucket("batchbucket1")
        bucket2 = self._client.create_bucket("batchbucket2")
        with self.assertRaises(NotFound):
            with self._client.batch():
                bucket1.delete()
                bucket1.delete()
                bucket2.delete()
        with self.assertRaises(NotFound):
            self._client.get_bucket("batchbucket1")
        with self.assertRaises(NotFound):
            self._client.get_bucket("batchbucket2")

    def test_batch_copy_same_bucket(self):
        bucket = self._client.create_bucket("batchbucket")
        source = []
        target = []
        for i in range(2):
            source.append(bucket.blob("a/{}.txt".format(i)))
            source[-1].upload_from_string("text {}".format(i))
            target.append(bucket.blob("b/{}.txt".format(i)))

        with self._client.batch():
            for i in range(2):
                bucket.copy_blob(source[i], bucket, target[i].name)

        blobs = list(self._client.list_blobs(bucket))
        self._assert_blob_list(source + target, blobs)
        for i in range(2):
            self.assertEqual(
                bucket.get_blob(target[i].name).download_as_bytes(),
                "text {}".format(i).encode("utf-8"),
            )

    def test_batch_copy_cross_bucket(self):
        src_bucket = self._client.create_bucket("batchsrc")
        dst_bucket = self._client.create_bucket("batchdst")
        source = src_bucket.blob("nested/file.txt")
        source.upload_from_string("cross-bucket-payload")

        with self._client.batch():
            src_bucket.copy_blob(source, dst_bucket, "copied/file.txt")

        copied = dst_bucket.get_blob("copied/file.txt")
        self.assertIsNotNone(copied)
        self.assertEqual(copied.download_as_bytes(), b"cross-bucket-payload")

    def test_batch_copy_nonexistent_source(self):
        bucket = self._client.create_bucket("batchbucket")
        missing = bucket.blob("does-not-exist.txt")
        with self.assertRaises(NotFound):
            with self._client.batch():
                bucket.copy_blob(missing, bucket, "dest.txt")

    def test_resumable_upload_small_chunk_size(self):
        content = b"a" * 10000000
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("resumable-test", chunk_size=256 * 1024)
        blob.upload_from_string(content)

        blob = bucket.get_blob("resumable-test")
        fetched_content = blob.download_as_bytes()
        self.assertEqual(len(fetched_content), len(content))
        self.assertEqual(fetched_content, content)

    def test_resumable_upload_large_file(self):
        content = b"abcde12345" * 2000000
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("resumable-test")
        blob.upload_from_string(content)

        blob = bucket.get_blob("resumable-test")
        fetched_content = blob.download_as_bytes()
        self.assertEqual(len(fetched_content), len(content))
        self.assertEqual(fetched_content, content)

    def test_resumable_chunked_write_via_blob_open(self):
        """Multi-chunk streaming write (issue #301).

        google-resumable-media sends Content-Range: bytes START-END/* until the
        final chunk, when the total size becomes known. Intermediate chunks must
        get HTTP 308, not 200.
        """
        chunk_size = 256 * 1024
        # More than two chunks so intermediate /* ranges are used.
        content = b"x" * (chunk_size * 3 + 123)
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("chunked-stream.bin", chunk_size=chunk_size)

        with blob.open("wb", chunk_size=chunk_size) as writer:
            offset = 0
            while offset < len(content):
                writer.write(content[offset : offset + chunk_size])
                offset += chunk_size

        fetched = bucket.get_blob("chunked-stream.bin").download_as_bytes()
        self.assertEqual(len(fetched), len(content))
        self.assertEqual(fetched, content)

    def test_resumable_chunked_known_total_via_upload_from_string(self):
        """Known-size multi-chunk upload (chunk_size smaller than object)."""
        chunk_size = 256 * 1024
        content = b"y" * (chunk_size * 2 + 50)
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("known-total.bin", chunk_size=chunk_size)
        blob.upload_from_string(content)
        fetched = bucket.get_blob("known-total.bin").download_as_bytes()
        self.assertEqual(fetched, content)

    def test_streaming_write_like_node_create_write_stream(self):
        """Streaming upload via blob.open (Node createWriteStream equivalent, #258)."""
        content = b"streamed-" + (b"data" * 10000)
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("streamed.txt")
        with blob.open("wb") as writer:
            # Write in several pieces like a passthrough stream.
            for i in range(0, len(content), 1024):
                writer.write(content[i : i + 1024])
        self.assertEqual(bucket.get_blob("streamed.txt").download_as_bytes(), content)

    def test_resumable_start_with_empty_json_body(self):
        """Node createWriteStream starts resumable with metadata body `{}`.

        Empty dict is falsy in Python; Request.data must cache it so the body
        is not re-read from the socket (which hangs the client).
        """
        self._client.create_bucket("testbucket")
        content = b"hello from node-like resumable"
        start = self._session.post(
            "http://localhost:9023/upload/storage/v1/b/testbucket/o",
            params={"name": "node-empty-meta.txt", "uploadType": "resumable"},
            headers={
                "Content-Type": "application/json",
                "X-Upload-Content-Type": "text/plain",
            },
            data=b"{}",
            timeout=5,
        )
        self.assertEqual(start.status_code, 200, start.text)
        location = start.headers.get("Location")
        self.assertTrue(location, "Location header required for resumable upload")

        # Node single-stream style: bytes START-*/TOTAL with full body.
        put = self._session.put(
            location,
            headers={
                "Content-Range": "bytes 0-*/{}".format(len(content)),
                "Content-Type": "text/plain",
            },
            data=content,
            timeout=5,
        )
        self.assertEqual(put.status_code, 200, put.text)
        blob = self._client.bucket("testbucket").get_blob("node-empty-meta.txt")
        self.assertEqual(blob.download_as_bytes(), content)

    def test_unprefixed_b_json_api_aliases(self):
        """Node STORAGE_EMULATOR_HOST uses /b/... without /storage/v1.

        @google-cloud/storage sets baseUrl to the emulator host only, so bucket
        and object calls hit POST /b, GET /b/bucket, GET /b/bucket/o/obj?alt=media.
        """
        # Create bucket via unprefixed path (like Node createBucket).
        created = self._session.post(
            "http://localhost:9023/b",
            params={"project": "test-project"},
            headers={"Content-Type": "application/json"},
            json={"name": "aliasbucket"},
            timeout=5,
        )
        self.assertEqual(created.status_code, 200, created.text)
        self.assertEqual(created.json()["name"], "aliasbucket")

        got = self._session.get("http://localhost:9023/b/aliasbucket", timeout=5)
        self.assertEqual(got.status_code, 200, got.text)
        self.assertEqual(got.json()["name"], "aliasbucket")

        content = b"via unprefixed json api"
        start = self._session.post(
            "http://localhost:9023/upload/storage/v1/b/aliasbucket/o",
            params={"name": "alias.txt", "uploadType": "resumable"},
            headers={
                "Content-Type": "application/json",
                "X-Upload-Content-Type": "text/plain",
            },
            data=b"{}",
            timeout=5,
        )
        self.assertEqual(start.status_code, 200, start.text)
        location = start.headers["Location"]
        put = self._session.put(
            location,
            headers={
                "Content-Range": "bytes 0-*/{}".format(len(content)),
                "Content-Type": "text/plain",
            },
            data=content,
            timeout=5,
        )
        self.assertEqual(put.status_code, 200, put.text)

        # Metadata + media download without /storage/v1 (Node download path).
        meta = self._session.get(
            "http://localhost:9023/b/aliasbucket/o/alias.txt", timeout=5
        )
        self.assertEqual(meta.status_code, 200, meta.text)
        self.assertEqual(meta.json()["name"], "alias.txt")

        media = self._session.get(
            "http://localhost:9023/b/aliasbucket/o/alias.txt",
            params={"alt": "media"},
            timeout=5,
        )
        self.assertEqual(media.status_code, 200, media.text)
        self.assertEqual(media.content, content)

        listed = self._session.get("http://localhost:9023/b/aliasbucket/o", timeout=5)
        self.assertEqual(listed.status_code, 200, listed.text)
        names = [item["name"] for item in listed.json().get("items", [])]
        self.assertIn("alias.txt", names)

    def test_upload_without_upload_prefix_does_not_crash(self):
        """Wrong path /storage/v1/.../o?uploadType= used by some clients (#258).

        Must not raise NoneType; either succeed (compat) or 501.
        """
        self._client.create_bucket("testbucket")
        boundary = "separator_string"
        body = (
            f"--{boundary}\r\n"
            "Content-Type: application/json; charset=UTF-8\r\n\r\n"
            '{"name":"my-document.txt"}\r\n'
            f"--{boundary}\r\n"
            "Content-Type: text/plain\r\n\r\n"
            "This is a text file.\r\n"
            f"--{boundary}--\r\n"
        )
        # Deliberately omit /upload prefix (as in issue #258 comment).
        response = self._session.post(
            "http://localhost:9023/storage/v1/b/testbucket/o?uploadType=multipart",
            headers={
                "Content-Type": f"multipart/related; boundary={boundary}",
            },
            data=body.encode("utf-8"),
        )
        # Compatibility: we accept uploadType on the JSON API objects path.
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["name"], "my-document.txt")
        bucket = self._client.bucket("testbucket")
        self.assertEqual(
            bucket.get_blob("my-document.txt").download_as_bytes(),
            b"This is a text file.",
        )

    def test_unsupported_method_returns_501_not_none_callable(self):
        """Path match without method must be 501, not 'NoneType is not callable'."""
        response = self._session.put(
            "http://localhost:9023/storage/v1/b/testbucket/o",
            data=b"nope",
        )
        self.assertEqual(response.status_code, 501)

    def test_empty_blob(self):
        bucket = self._client.create_bucket("testbucket")
        bucket.blob("empty_blob").open("w").close()

        blob = bucket.get_blob("empty_blob")
        fetched_content = blob.download_as_bytes()
        self.assertEqual(fetched_content, b"")

    def test_signed_url_download(self):
        content = b"The quick brown fox jumps over the lazy dog"
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("signed-download")
        blob.upload_from_string(content, content_type="text/mycustom")

        url = blob.generate_signed_url(
            api_access_endpoint="http://localhost:9023",
            credentials=FakeSigningCredentials(),
            version="v4",
            expiration=datetime.timedelta(minutes=15),
            method="GET",
        )

        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, content)
        self.assertEqual(response.headers["content-type"], "text/mycustom")

    def test_signed_url_download_with_content_disposition(self):
        content = b"The quick brown fox jumps over the lazy dog"
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("signed-download")
        blob.upload_from_string(content, content_type="text/mycustom")

        requested_filename = "requested_filename.cst2"
        response_disposition = f'attachment; filename="{requested_filename}"'

        url = blob.generate_signed_url(
            api_access_endpoint="http://localhost:9023",
            credentials=FakeSigningCredentials(),
            version="v4",
            expiration=datetime.timedelta(minutes=15),
            response_disposition=response_disposition,
            method="GET",
        )

        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, content)
        self.assertEqual(
            response.headers["content-disposition"], f"{response_disposition}"
        )
        self.assertEqual(response.headers["content-type"], "text/mycustom")

    def test_url_generation_for_browser(self):
        self.skipTest("Used to test browser functionality with URL, not API.")
        os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:8080"
        content = b"The quick brown fox jumps over the lazy dog"

        # Cloud Storage uses environment variables to configure API endpoints for
        # file upload - which is read at module import time
        from google.cloud import storage

        http = requests.Session()

        client = storage.Client(
            project="[PROJECT]",
            _http=http,
            client_options={"api_endpoint": "http://localhost:8080"},
        )

        bucket = client.create_bucket("testbucket")

        blob = bucket.blob("signed-download")
        blob.upload_from_string(content, content_type="text/html")

        requested_filename = "requested_filename.cst2"
        response_disposition = f'attachment; filename="{requested_filename}"'

        url = blob.generate_signed_url(
            api_access_endpoint="http://localhost:8080",
            credentials=FakeSigningCredentials(),
            version="v4",
            expiration=datetime.timedelta(minutes=15),
            response_disposition=response_disposition,
            method="GET",
        )
        print(url)

    def test_signed_url_upload(self):
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("signed-upload")
        url = blob.generate_signed_url(
            api_access_endpoint="http://localhost:9023",
            credentials=FakeSigningCredentials(),
            version="v4",
            expiration=datetime.timedelta(minutes=15),
            method="PUT",
        )
        with open(TEST_TEXT, "rb") as file:
            headers = {"Content-type": "text/plain"}
            response = requests.put(url, data=file, headers=headers)
            self.assertEqual(response.status_code, 200)

            blob_content = blob.download_as_bytes()
            file.seek(0)
            self.assertEqual(blob_content, file.read())
            self.assertEqual(blob.content_type, "text/plain")

    def test_signed_url_upload_to_nonexistent_bucket(self):
        bucket = self._client.bucket("non-existent-test-bucket")
        blob = bucket.blob("idonotexisteither")
        url = blob.generate_signed_url(
            api_access_endpoint="http://localhost:9023",
            credentials=FakeSigningCredentials(),
            version="v4",
            expiration=datetime.timedelta(minutes=15),
            method="PUT",
        )
        with open(TEST_TEXT, "rb") as file:
            response = requests.put(url, data=file)
            self.assertEqual(response.status_code, 404)

    def test_initiate_resumable_upload_without_metadata(self):
        url = "http://127.0.0.1:9023/upload/storage/v1/b/test_bucket/o?"
        url += "uploadType=resumable&name=test_file"
        self._client.create_bucket("test_bucket")
        headers = {"Content-type": "application/json"}
        response = requests.post(url, headers=headers)
        self.assertEqual(response.status_code, 200)

    def test_media_upload_without_metadata(self):
        url = "http://127.0.0.1:9023/upload/storage/v1/b/test_bucket/o?"
        url += "uploadType=media&name=test_file&contentEncoding=text%2Fplain"
        bucket = self._client.create_bucket("test_bucket")
        with open(TEST_TEXT, "rb") as file:
            headers = {"Content-type": "text/html"}
            response = requests.post(url, data=file, headers=headers)
            self.assertEqual(response.status_code, 200)
            blob = bucket.blob("test_file")
            blob_content = blob.download_as_bytes()
            file.seek(0)
            self.assertEqual(blob_content, file.read())
            self.assertEqual(blob.content_type, "text/plain")

    def test_upload_from_file_content_type_json(self):
        file_name = "test.json"
        content = b'[{"a": 1}]'
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob(file_name)

        with NamedTemporaryFile() as temp_file:
            temp_file.write(content)
            temp_file.flush()
            temp_file.seek(0)
            blob.upload_from_file(temp_file, content_type="application/json")

        blob = bucket.get_blob(file_name)
        self.assertEqual(blob.name, file_name)
        self.assertEqual(blob.download_as_bytes(), content)

    def test_upload_from_file_with_metadata(self):
        file_name = "test.json"
        content = b'[{"a": 1}]'
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob(file_name)
        blob.metadata = {"foo": "bar"}

        with NamedTemporaryFile() as temp_file:
            temp_file.write(content)
            temp_file.flush()
            temp_file.seek(0)
            blob.upload_from_file(temp_file, content_type="application/json")

        blob = bucket.get_blob(file_name)
        self.assertEqual(blob.name, file_name)
        self.assertEqual(blob.download_as_bytes(), content)
        self.assertEqual(blob.metadata, {"foo": "bar"})


class HttpEndpointsTest(ServerBaseCase):
    """Tests for the HTTP endpoints defined by server.HANDLERS."""

    def _url(self, path):
        return os.environ["STORAGE_EMULATOR_HOST"] + path

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

    def test_download_by_dl_api_url(self):
        """Objects should be downloadable over HTTP from the emulator client."""
        content = "Here is some content 123"
        bucket = self._client.create_bucket("bucket")
        blob = bucket.blob("something.txt")
        blob.upload_from_string(content)

        url = self._url("/download/storage/v1/b/bucket/o/something.txt")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, content.encode("utf-8"))

    def test_download_by_api_media_url(self):
        """Objects should be downloadable over HTTP from the emulator client."""
        content = "Here is some content 456"
        bucket = self._client.create_bucket("bucket")
        blob = bucket.blob("something.txt")
        blob.upload_from_string(content)

        url = self._url("/storage/v1/b/bucket/o/something.txt")
        response = requests.get(url, params={"alt": "media"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, content.encode("utf-8"))

    def test_download_file_within_folder(self):
        """Cloud Storage allows folders within buckets, so the download URL should allow for this."""
        content = "Here is some content"
        bucket = self._client.create_bucket("yetanotherbucket")
        blob = bucket.blob("folder/contain~ing/something~v-1.0.α.txt")
        blob.upload_from_string(content)

        url = self._url("/yetanotherbucket/folder/contain~ing/something~v-1.0.α.txt")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, content.encode("utf-8"))

    def test_wipe(self):
        """Objects should wipe the data"""
        storage_path = os.path.join(STORAGE_BASE, STORAGE_DIR)
        content = "Here is some content"
        bucket = self._client.create_bucket("anotherbucket1")
        blob = bucket.blob("something.txt")
        blob.upload_from_string(content)

        url = self._url("/wipe")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(len(os.listdir(storage_path)) == 0)

    def test_wipe_keep_buckets(self):
        """Objects should wipe the data but keep the root buckets"""
        blob_path = "something.txt"
        bucket_name = "anewone"
        content = "Here is some content"
        bucket = self._client.create_bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_string(content)

        url = self._url("/wipe?keep-buckets=true")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)

        fetched_bucket = self._client.get_bucket(bucket_name)
        self.assertEqual(fetched_bucket.name, bucket.name)
        with self.assertRaises(NotFound):
            fetched_bucket.blob(blob_path).download_as_text()
