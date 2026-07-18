from unittest import TestCase

from gcp_storage_emulator.exceptions import BadRequest
from gcp_storage_emulator.handlers.objects import _parse_resumable_content_range
from gcp_storage_emulator.storage import Storage


class ResumableContentRangeTests(TestCase):
    def test_parse_python_and_node_forms(self):
        self.assertEqual(
            _parse_resumable_content_range("bytes 0-999/5000"),
            {"kind": "chunk", "start": 0, "end": 999, "total": 5000},
        )
        self.assertEqual(
            _parse_resumable_content_range("bytes 0-999/*"),
            {"kind": "chunk", "start": 0, "end": 999, "total": None},
        )
        # Node createWriteStream single-request style
        self.assertEqual(
            _parse_resumable_content_range("bytes 0-*/1500"),
            {"kind": "star_end", "start": 0, "end": None, "total": 1500},
        )
        self.assertEqual(
            _parse_resumable_content_range("bytes 100-*/*"),
            {"kind": "star_end", "start": 100, "end": None, "total": None},
        )
        self.assertEqual(
            _parse_resumable_content_range("bytes */*"),
            {"kind": "status", "start": None, "end": None, "total": None},
        )
        self.assertEqual(
            _parse_resumable_content_range("bytes */1500"),
            {"kind": "status", "start": None, "end": None, "total": 1500},
        )
        self.assertIsNone(_parse_resumable_content_range(""))
        self.assertIsNone(_parse_resumable_content_range("invalid"))

    def test_storage_star_end_completes_when_total_known(self):
        storage = Storage(use_memory_fs=True)
        storage.create_bucket("b", {"name": "b"})
        upload_id = storage.create_resumable_upload(
            "b", "f.bin", {"bucket": "b", "name": "f.bin"}
        )
        content = b"hello-node-stream"
        # Simulate bytes 0-*/N with full body
        data = storage.add_to_resumable_upload(
            upload_id,
            content,
            total_size=len(content),
            expected_start=0,
        )
        self.assertEqual(data, content)

    def test_storage_unknown_total_stays_incomplete(self):
        storage = Storage(use_memory_fs=True)
        storage.create_bucket("b", {"name": "b"})
        upload_id = storage.create_resumable_upload(
            "b", "f.bin", {"bucket": "b", "name": "f.bin"}
        )
        data = storage.add_to_resumable_upload(
            upload_id, b"partial", total_size=None, expected_start=0
        )
        self.assertIsNone(data)
        self.assertEqual(storage.get_resumable_byte_count(upload_id), 7)

    def test_storage_rejects_bad_start(self):
        storage = Storage(use_memory_fs=True)
        storage.create_bucket("b", {"name": "b"})
        upload_id = storage.create_resumable_upload(
            "b", "f.bin", {"bucket": "b", "name": "f.bin"}
        )
        storage.add_to_resumable_upload(
            upload_id, b"abc", total_size=None, expected_start=0
        )
        with self.assertRaises(BadRequest):
            storage.add_to_resumable_upload(
                upload_id, b"x", total_size=None, expected_start=0
            )
