import json
import os
from unittest import TestCase as BaseTestCase

from gcp_storage_emulator.exceptions import NotFound
from gcp_storage_emulator.settings import STORAGE_BASE, STORAGE_DIR
from gcp_storage_emulator.storage import Storage


def _get_meta_path():
    return os.path.join(os.getcwd(), STORAGE_BASE, STORAGE_DIR, ".meta")


class StorageOSFSTests(BaseTestCase):
    def setUp(self):
        self.storage = Storage()
        self.storage.wipe()
        self.storage.create_bucket("a_bucket_name", {})

    def tearDown(self):
        self.storage.wipe()

    def test_get_bucket_reads_from_meta(self):
        meta_path = _get_meta_path()
        buckets = {"key": "a"}

        with open(meta_path, "w") as file:
            json.dump(
                {
                    "buckets": buckets,
                },
                file,
            )

        # Force a re-read from file, this is usually done in the constructor
        self.storage._read_config_from_file()
        self.assertEqual(self.storage.get_bucket("key"), "a")

    def test_get_file_obj_reads_from_meta(self):
        meta_path = _get_meta_path()
        objects = {"key": {"inner_key": "a"}}

        with open(meta_path, "w") as file:
            json.dump(
                {
                    "objects": objects,
                },
                file,
            )

        # Force a re-read from file, this is usually done in the constructor
        self.storage._read_config_from_file()
        self.assertEqual(self.storage.get_file_obj("key", "inner_key"), "a")

    def test_get_file_obj_not_found(self):
        with self.assertRaises(NotFound):
            self.storage.get_file_obj("a_bucket", "a_file")

        self.storage.create_bucket("a_bucket", {})
        with self.assertRaises(NotFound):
            self.storage.get_file_obj("a_bucket", "a_file")

    def test_get_file_not_found(self):
        with self.assertRaises(NotFound):
            self.storage.get_file("a_bucket", "a_file")

        self.storage.create_bucket("a_bucket", {})
        with self.assertRaises(NotFound):
            self.storage.get_file("a_bucket", "a_file")

    def test_create_bucket_stores_meta(self):
        bucket_obj = {"key": "val"}
        self.storage.create_bucket("a_bucket", bucket_obj)

        meta_path = _get_meta_path()
        with open(meta_path, "r") as file:
            meta = json.load(file)
            self.assertEqual(meta["buckets"]["a_bucket"], bucket_obj)

    def test_create_file_stores_content(self):
        test_file = os.path.join(
            os.getcwd(), STORAGE_BASE, STORAGE_DIR, "a_bucket_name", "file_name.txt"
        )
        content = "Łukas is a great developer".encode("utf8")
        file_obj = {}
        self.storage.create_file("a_bucket_name", "file_name.txt", content, file_obj)

        with open(test_file, "rb") as file:
            read_content = file.read()
            self.assertEqual(read_content, content)

    def test_create_file_stores_meta(self):
        content = "Łukas is a great developer".encode("utf8")
        file_obj = {"key": "val"}
        self.storage.create_file("a_bucket_name", "file_name.txt", content, file_obj)
        meta_path = _get_meta_path()
        with open(meta_path, "r") as file:
            meta = json.load(file)
            self.assertEqual(
                meta["objects"]["a_bucket_name"]["file_name.txt"], file_obj
            )

    def test_create_resumable_upload_stores_meta(self):
        file_obj = {"key": "val"}
        file_id = self.storage.create_resumable_upload(
            "a_bucket_name", "file_name.png", file_obj
        )
        meta_path = _get_meta_path()
        with open(meta_path, "r") as file:
            meta = json.load(file)
            self.assertEqual(meta["resumable"][file_id], file_obj)

    def test_file_ids_dont_clash(self):
        file_obj = {"key": "val"}
        file_id_1 = self.storage.create_resumable_upload(
            "a_bucket_name", "file_name.png", file_obj
        )
        file_id_2 = self.storage.create_resumable_upload(
            "a_bucket_name", "file_name.png", file_obj
        )
        self.assertNotEqual(file_id_1, file_id_2)

    def test_create_file_for_resumable_upload(self):
        test_file = os.path.join(
            os.getcwd(), STORAGE_BASE, STORAGE_DIR, "a_bucket_name", "file_name.png"
        )
        content = b"Randy is also a great developer"
        file_obj = {"bucket": "a_bucket_name", "name": "file_name.png"}
        file_id = self.storage.create_resumable_upload(
            "a_bucket_name", "file_name.png", file_obj
        )
        self.assertEqual(self.storage.get_resumable_file_obj(file_id), file_obj)
        self.storage.create_file(
            file_obj["bucket"], file_obj["name"], content, file_obj, file_id
        )

        with open(test_file, "rb") as file:
            read_content = file.read()
            self.assertEqual(read_content, content)

        with open(_get_meta_path(), "r") as file:
            meta = json.load(file)
            self.assertEqual(
                meta["objects"]["a_bucket_name"]["file_name.png"], file_obj
            )
            self.assertEqual(meta["resumable"], {})

    def test_delete_bucket_stores_meta(self):
        bucket_obj = {"key": "val"}
        self.storage.create_bucket("a_bucket", bucket_obj)

        self.storage.delete_bucket("a_bucket")

        meta_path = _get_meta_path()
        with open(meta_path, "r") as file:
            meta = json.load(file)
            self.assertIsNone(meta["buckets"].get("a_bucket"))

    def test_wipe(self):
        bucket_a_obj = {"key_a": "val_a"}
        bucket_b_obj = {"key_b": "valb_"}
        self.storage.create_bucket("bucket_a", bucket_a_obj)
        self.storage.create_bucket("bucket_b", bucket_b_obj)
        self.storage.wipe()
        meta_path = _get_meta_path()
        self.assertFalse(os.path.isfile(meta_path))

    def test_wipe_keep_buckets(self):
        bucket_a_obj = {"key_a": "val_a"}
        bucket_b_obj = {"key_b": "valb_"}
        self.storage.create_bucket("bucket_a", bucket_a_obj)
        self.storage.create_bucket("bucket_b", bucket_b_obj)
        self.storage.wipe(keep_buckets=True)
        meta_path = _get_meta_path()

        with open(meta_path, "r") as file:
            meta = json.load(file)
            self.assertEqual(meta["buckets"]["bucket_a"], bucket_a_obj)
            self.assertEqual(meta["buckets"]["bucket_b"], bucket_b_obj)
            self.assertEqual(meta["objects"], {})
            self.assertEqual(meta["resumable"], {})

    def test_without_absolute_path(self):
        with self.assertRaises(ValueError):
            _ = Storage(data_dir="test")
