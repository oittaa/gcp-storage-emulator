import datetime
import os
from io import BytesIO
from tempfile import NamedTemporaryFile
from unittest import TestCase as BaseTestCase

import fs
import requests
from google.api_core.exceptions import BadRequest, Conflict, NotFound

from gcp_storage_emulator.server import create_server
from gcp_storage_emulator.settings import STORAGE_BASE, STORAGE_DIR


def _get_storage_client(http):
    """Gets a python storage client"""
    os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:9023"

    # Cloud storage uses environment variables to configure api endpoints for
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
        cls._server.stop()

    def setUp(self):
        self._session = requests.Session()
        self._client = _get_storage_client(self._session)
        ObjectsTests._server.wipe()


class BucketsTests(BaseTestCase):

    @classmethod
    def setUpClass(cls):
        cls._server = create_server("localhost", 9023, in_memory=False)
        cls._server.start()

    @classmethod
    def tearDownClass(cls):
        cls._server.stop()

    def setUp(self):
        BucketsTests._server.wipe()
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

        with fs.open_fs(STORAGE_BASE + STORAGE_DIR) as pwd:
            self.assertFalse(pwd.exists("bucket_name"))

    def test_bucket_delete_non_existing(self):
        # client.bucket doesn't create the actual bucket resource remotely,
        # it only instantiate it in the local client
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

        with fs.open_fs(STORAGE_BASE + STORAGE_DIR) as pwd:
            self.assertFalse(pwd.exists('bucket_name'))
    # TODO: test delete-force


class DefaultBucketTests(BaseTestCase):
    def tearDown(self):
        if self._server:
            self._server.stop()
        return super().tearDown()

    def test_bucket_created(self):
        self._server = create_server("localhost", 9023, in_memory=True, default_bucket="example.appspot.com")
        self._server.start()
        self._session = requests.Session()
        self._client = _get_storage_client(self._session)
        self._client.get_bucket("example.appspot.com")


class ObjectsTests(ServerBaseCase):

    def test_upload_from_string(self):
        content = "this is the content of the file\n"
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("testblob-name.txt")
        blob.upload_from_string(content)

        with fs.open_fs(STORAGE_BASE + STORAGE_DIR) as pwd:
            read_content = pwd.readtext("testbucket/testblob-name.txt")
            self.assertEqual(read_content, content)

    def test_upload_from_text_file(self):
        text_test = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_text.txt')
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("test_text.txt")
        with open(text_test, "rb") as file:
            blob.upload_from_file(file)

            with fs.open_fs(STORAGE_BASE + STORAGE_DIR) as pwd:
                read_content = pwd.readtext("testbucket/test_text.txt")

        with open(text_test, "rb") as file:
            expected_content = str(file.read(), encoding="utf-8")
            self.assertEqual(read_content, expected_content)

    def test_upload_from_bin_file(self):
        test_binary = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_binary.png')
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("binary.png")
        with open(test_binary, "rb") as file:
            blob.upload_from_file(file)

        with fs.open_fs(STORAGE_BASE + STORAGE_DIR) as pwd:
            read_content = pwd.readbytes("testbucket/binary.png")

        with open(test_binary, "rb") as file:
            expected_content = file.read()
            self.assertEqual(read_content, expected_content)

    def test_upload_from_bin_file_cr_lf(self):
        content = b'\r\rheeeeei\r\n'
        test_binary = BytesIO(content)
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("binary_cr.png")

        blob.upload_from_file(
            test_binary, size=len(content)
        )

        with fs.open_fs(STORAGE_BASE + STORAGE_DIR) as pwd:
            read_content = pwd.readbytes("testbucket/binary_cr.png")

        self.assertEqual(read_content, content)

    def test_upload_from_file_name(self):
        test_binary = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_binary.png')
        file_name = "test_binary.png"

        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob(file_name)
        blob.upload_from_filename(test_binary)
        blob = bucket.get_blob(file_name)
        with NamedTemporaryFile() as temp_file:
            blob.download_to_filename(temp_file.name)
            with open(test_binary, "rb") as orig_file:
                self.assertEqual(temp_file.read(), orig_file.read())

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

    def test_get_nonexistant(self):
        bucket = self._client.create_bucket("testbucket")
        res = bucket.get_blob("idonotexist")

        self.assertIsNone(res)

        blob = bucket.blob("iexist")
        blob.upload_from_string("some_fake_content")
        res = bucket.get_blob("idonotexist")

        self.assertIsNone(res)

    def test_download_as_bytes(self):
        content = "The quick brown fox jumps over the lazy dog\n"
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("iexist")
        blob.upload_from_string(content)

        blob = bucket.get_blob("iexist")
        fetched_content = blob.download_as_bytes()
        self.assertEqual(fetched_content, content.encode('utf-8'))

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
        metadata = {"Color": 'Pink'}

        blob = bucket.blob("testblob")
        blob.metadata = metadata
        blob.upload_from_string(content)
        blob.reload()
        self.assertEqual(blob.metadata, metadata)

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
        self.assertEqual(blob.content_encoding, "gzip")
        blob.content_encoding = ""
        blob.patch()
        blob.reload()
        self.assertEqual(blob.content_encoding, "")

    def test_valid_md5_hash(self):
        content = b"test"
        md5_hash = "CY9rzUYh03PK3k6DJie09g=="
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("hashtest")
        blob.md5_hash = md5_hash
        blob.upload_from_string(content)
        download_blob = bucket.get_blob("hashtest")
        self.assertEqual(download_blob.download_as_bytes(), content)
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
        self.assertEqual(download_blob.download_as_bytes(), content)
        self.assertEqual(download_blob.crc32c, crc32c_hash)

    def test_invalid_crc32c_hash(self):
        content = b"Hello World"
        bucket = self._client.create_bucket("testbucket")
        blob = bucket.blob("hashtest")
        blob.crc32c = "deadbeef"
        with self.assertRaises(BadRequest):
            blob.upload_from_string(content)

    def test_download_binary_to_file(self):
        test_binary = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_binary.png')
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
        test_text = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_text.txt')
        bucket = self._client.create_bucket("testbucket")

        blob = bucket.blob("text.txt")
        with open(test_text, "rb") as file:
            blob.upload_from_file(file, content_type="text/plain; charset=utf-8")

        blob = bucket.get_blob("text.txt")
        fetched_file = BytesIO()
        blob.download_to_file(fetched_file)

        with open(test_text, "rb") as file:
            self.assertEqual(fetched_file.getvalue(), file.read())

    def test_delete_object(self):
        bucket = self._client.create_bucket("bucket_name")
        blob = bucket.blob("canttouchme.txt")
        blob.upload_from_string("File content")

        with fs.open_fs(STORAGE_BASE + STORAGE_DIR) as pwd:
            self.assertTrue(pwd.exists("bucket_name/canttouchme.txt"))
            blob.delete()

            self.assertIsNone(bucket.get_blob("cantouchme.txt"))
            self.assertFalse(pwd.exists("bucket_name/canttouchme.txt"))

    def test_create_within_directory(self):
        bucket = self._client.create_bucket("bucket_name")
        blob = bucket.blob("this/is/a/nested/file.txt")
        blob.upload_from_string("Not even joking!")

        with fs.open_fs(STORAGE_BASE + STORAGE_DIR) as pwd:
            read_content = pwd.readtext("bucket_name/this/is/a/nested/file.txt")
            self.assertEqual(read_content, "Not even joking!")

    def test_create_within_multiple_time_does_not_break(self):
        bucket = self._client.create_bucket("bucket_name")
        blob = bucket.blob("this/is/a/nested/file.txt")
        blob.upload_from_string("Not even joking!")

        bucket.blob("this/is/another/nested/file.txt")
        blob.upload_from_string("Yet another one")

        with fs.open_fs(STORAGE_BASE + STORAGE_DIR) as pwd:
            self.assertTrue(pwd.exists("bucket_name/this/is/a/nested/file.txt"))

    def _assert_blob_list(self, expected, actual):
        self.assertEqual(
            [b.name for b in expected],
            [b.name for b in actual])

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

        blobs = self._client.list_blobs(bucket, prefix='a')

        self._assert_blob_list(blobs, [blob_1, blob_2])

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

        blobs = self._client.list_blobs(bucket, prefix='a', delimiter='/')

        self._assert_blob_list(blobs, [blob_1, blob_2])

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


class HttpEndpointsTest(ServerBaseCase):
    """ Tests for the HTTP endpoints defined by server.HANDLERS. """

    def _url(self, path):
        return os.environ["STORAGE_EMULATOR_HOST"] + path

    def test_download_by_url(self):
        """ Objects should be downloadable over HTTP from the emulator client. """
        content = "Here is some content"
        bucket = self._client.create_bucket("anotherbucket")
        blob = bucket.blob("something.txt")
        blob.upload_from_string(content)

        url = self._url("/anotherbucket/something.txt")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, content.encode('utf-8'))

    def test_download_file_within_folder(self):
        """ Cloud Storage allows folders within buckets, so the download URL should allow for this.
        """
        content = "Here is some content"
        bucket = self._client.create_bucket("yetanotherbucket")
        blob = bucket.blob("folder/contain~ing/something~v-1.0.α.txt")
        blob.upload_from_string(content)

        url = self._url("/yetanotherbucket/folder/contain~ing/something~v-1.0.α.txt")
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, content.encode('utf-8'))
