import datetime
import json
import logging
import os
import shutil
from hashlib import sha256
from pathlib import Path
from typing import Dict, List, Optional, Set

from gcp_storage_emulator.exceptions import Conflict, NotFound
from gcp_storage_emulator.settings import STORAGE_BASE, STORAGE_DIR

# Real buckets can't start with an underscore
RESUMABLE_DIR = "_resumable"

logger = logging.getLogger(__name__)


class _FileStore:
    """Minimal disk or in-memory store rooted at a logical directory.

    Paths use forward slashes and are relative to the store root (no leading slash).
    """

    def __init__(self, *, use_memory: bool, root: Optional[Path] = None) -> None:
        self._use_memory = use_memory
        if use_memory:
            self._files: Dict[str, bytes] = {}
            self._dirs: Set[str] = {""}
            self._root: Optional[Path] = None
        else:
            if root is None:
                raise ValueError("root is required for disk storage")
            self._root = root
            self._root.mkdir(parents=True, exist_ok=True)
            self._files = {}
            self._dirs = set()

    @staticmethod
    def _norm(path: str) -> str:
        path = path.replace("\\", "/").strip("/")
        parts = [p for p in path.split("/") if p and p != "."]
        if any(p == ".." for p in parts):
            raise ValueError(f"invalid path: {path!r}")
        return "/".join(parts)

    def _disk_path(self, rel: str) -> Path:
        assert self._root is not None
        rel = self._norm(rel)
        if not rel:
            return self._root
        return self._root.joinpath(*rel.split("/"))

    def makedirs(self, rel: str) -> None:
        rel = self._norm(rel)
        if self._use_memory:
            if not rel:
                return
            parts = rel.split("/")
            for i in range(len(parts)):
                self._dirs.add("/".join(parts[: i + 1]))
            return
        self._disk_path(rel).mkdir(parents=True, exist_ok=True)

    def write_bytes(self, rel: str, content: bytes) -> None:
        rel = self._norm(rel)
        parent = self._norm(str(Path(rel).parent)) if "/" in rel else ""
        if parent:
            self.makedirs(parent)
        if self._use_memory:
            if parent:
                self._dirs.add(parent)
            self._files[rel] = content
            return
        path = self._disk_path(rel)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    def read_bytes(self, rel: str) -> bytes:
        rel = self._norm(rel)
        if self._use_memory:
            try:
                return self._files[rel]
            except KeyError as err:
                raise FileNotFoundError(rel) from err
        path = self._disk_path(rel)
        if not path.is_file():
            raise FileNotFoundError(rel)
        return path.read_bytes()

    def exists_file(self, rel: str) -> bool:
        rel = self._norm(rel)
        if self._use_memory:
            return rel in self._files
        return self._disk_path(rel).is_file()

    def remove_file(self, rel: str) -> None:
        rel = self._norm(rel)
        if self._use_memory:
            try:
                del self._files[rel]
            except KeyError as err:
                raise FileNotFoundError(rel) from err
            return
        path = self._disk_path(rel)
        if not path.is_file():
            raise FileNotFoundError(rel)
        path.unlink()

    def remove_tree(self, rel: str) -> None:
        rel = self._norm(rel)
        if self._use_memory:
            prefix = f"{rel}/" if rel else ""
            if (
                rel
                and rel not in self._dirs
                and not any(p == rel or p.startswith(prefix) for p in self._files)
            ):
                # match "no folder" semantics for missing trees
                if not any(p.startswith(prefix) or p == rel for p in self._files):
                    raise FileNotFoundError(rel)
            self._files = {
                p: data
                for p, data in self._files.items()
                if not (p == rel or (prefix and p.startswith(prefix)))
            }
            self._dirs = {
                d
                for d in self._dirs
                if d != rel and not (prefix and d.startswith(prefix))
            }
            self._dirs.add("")
            return
        path = self._disk_path(rel)
        if not path.exists():
            raise FileNotFoundError(rel)
        if path.is_file():
            path.unlink()
        else:
            shutil.rmtree(path)

    def listdir(self, rel: str = "") -> List[str]:
        rel = self._norm(rel)
        if self._use_memory:
            prefix = f"{rel}/" if rel else ""
            names: Set[str] = set()
            for path in list(self._files) + list(self._dirs):
                if rel:
                    if path == rel:
                        continue
                    if not path.startswith(prefix):
                        continue
                    rest = path[len(prefix) :]
                else:
                    rest = path
                if not rest:
                    continue
                names.add(rest.split("/", 1)[0])
            return sorted(names)
        path = self._disk_path(rel) if rel else self._root
        assert path is not None
        if not path.is_dir():
            raise FileNotFoundError(rel)
        return sorted(entry.name for entry in path.iterdir())


class Storage:
    def __init__(self, use_memory_fs=False, data_dir=None):
        if not data_dir:
            data_dir = STORAGE_BASE
        if not os.path.isabs(data_dir):
            raise ValueError(f"{data_dir!r} must be an absolute path")

        self._data_dir = data_dir
        self._use_memory_fs = use_memory_fs
        if use_memory_fs:
            self._store = _FileStore(use_memory=True)
        else:
            os.makedirs(self._data_dir, exist_ok=True)
            root = Path(self._data_dir) / STORAGE_DIR
            self._store = _FileStore(use_memory=False, root=root)

        self._read_config_from_file()

    def _write_config_to_file(self):
        data = {
            "buckets": self.buckets,
            "objects": self.objects,
            "resumable": self.resumable,
        }
        self._store.write_bytes(".meta", json.dumps(data, indent=2).encode("utf-8"))

    def _read_config_from_file(self):
        try:
            raw = self._store.read_bytes(".meta")
        except FileNotFoundError:
            self.buckets = {}
            self.objects = {}
            self.resumable = {}
            return
        data = json.loads(raw.decode("utf-8"))
        self.buckets = data.get("buckets")
        self.objects = data.get("objects")
        self.resumable = data.get("resumable")

    def _object_path(self, bucket_name, file_name):
        file_name = file_name.replace("\\", "/").lstrip("/")
        return f"{bucket_name}/{file_name}"

    def get_storage_base(self):
        """Returns the storage base location (for compatibility).

        Disk mode returns the configured data directory; in-memory mode returns
        the string ``"memory"``.
        """
        if self._use_memory_fs:
            return "memory"
        return self._data_dir

    def get_bucket(self, bucket_name):
        """Get the bucket resourec object given the bucket name

        Arguments:
            bucket_name {str} -- Name of the bucket

        Returns:
            dict -- GCS-like Bucket resource
        """

        return self.buckets.get(bucket_name)

    def get_file_list(self, bucket_name, prefix=None, delimiter=None):
        """Lists all the blobs in the bucket that begin with the prefix.

        This can be used to list all blobs in a "folder", e.g. "public/".

        The delimiter argument can be used to restrict the results to only the
        "files" in the given "folder". Without the delimiter, the entire tree under
        the prefix is returned. For example, given these blobs:

            a/1.txt
            a/b/2.txt

        If you just specify prefix = 'a', you'll get back:

            a/1.txt
            a/b/2.txt

        However, if you specify prefix='a' and delimiter='/', you'll get back:

            a/1.txt

        Additionally, the same request will return blobs.prefixes populated with:

            a/b/

        Source: https://cloud.google.com/storage/docs/listing-objects#storage-list-objects-python
        """

        if bucket_name not in self.buckets:
            raise NotFound

        prefix_len = 0
        prefixes = []
        bucket_objects = self.objects.get(bucket_name, {})
        if prefix:
            prefix_len = len(prefix)
            objs = list(
                file_object
                for file_name, file_object in bucket_objects.items()
                if file_name.startswith(prefix)
                and (not delimiter or delimiter not in file_name[prefix_len:])
            )
        else:
            objs = list(bucket_objects.values())
        if delimiter:
            prefixes = list(
                file_name[:prefix_len]
                + file_name[prefix_len:].split(delimiter, 1)[0]
                + delimiter
                for file_name in list(bucket_objects)
                if file_name.startswith(prefix or "")
                and delimiter in file_name[prefix_len:]
            )
        return objs, prefixes

    def create_bucket(self, bucket_name, bucket_obj):
        """Create a bucket object representation and save it to the current fs

        Arguments:
            bucket_name {str} -- Name of the GCS bucket
            bucket_obj {dict} -- GCS-like Bucket resource

        Returns:
            [type] -- [description]
        """

        self.buckets[bucket_name] = bucket_obj
        self._store.makedirs(bucket_name)
        self._write_config_to_file()
        return bucket_obj

    def create_file(self, bucket_name, file_name, content, file_obj, file_id=None):
        """Create a text file given a string content

        Arguments:
            bucket_name {str} -- Name of the bucket to save to
            file_name {str} -- File name used to store data
            content {bytes} -- Content of the file to write
            file_obj {dict} -- GCS-like Object resource
            file_id {str} -- Resumable file id

        Raises:
            NotFound: Raised when the bucket doesn't exist
        """

        if bucket_name not in self.buckets:
            raise NotFound

        self._store.write_bytes(self._object_path(bucket_name, file_name), content)
        bucket_objects = self.objects.get(bucket_name, {})
        bucket_objects[file_name] = file_obj
        self.objects[bucket_name] = bucket_objects
        if file_id:
            self.delete_resumable_file_obj(file_id)
            self._delete_file(RESUMABLE_DIR, self.safe_id(file_id))
        self._write_config_to_file()

    def create_resumable_upload(self, bucket_name, file_name, file_obj):
        """Initiate the necessary data to support partial upload.

        This doesn't fully support partial upload, but expect the secondary PUT
        call to send all the data in one go.

        Basically, we try to comply to the bare minimum to the API described in
        https://cloud.google.com/storage/docs/performing-resumable-uploads ignoring
        any potential network failures

        Arguments:
            bucket_name {string} -- Name of the bucket to save to
            file_name {string} -- File name used to store data
            file_obj {dict} -- GCS Object resource

        Raises:
            NotFound: Raised when the bucket doesn't exist

        Returns:
            str -- id of the resumable upload session (`upload_id`)
        """

        if bucket_name not in self.buckets:
            raise NotFound

        file_id = "{}:{}:{}".format(bucket_name, file_name, datetime.datetime.now())
        self.resumable[file_id] = file_obj
        self._write_config_to_file()
        return file_id

    def add_to_resumable_upload(self, file_id, content, total_size):
        """Add data to partial resumable download.

        We can't use 'seek' to append since memory store seems to erase
        everything in those cases. That's why the previous part is loaded
        and rewritten again.

         Arguments:
            file_id {str} -- Resumable file id
            content {bytes} -- Content of the file to write
            total_size {int} -- Total object size


        Raises:
            NotFound: Raised when the object doesn't exist

        Returns:
            bytes -- Raw content of the file if completed, None otherwise
        """
        safe_id = self.safe_id(file_id)
        try:
            file_content = self.get_file(RESUMABLE_DIR, safe_id, False)
        except NotFound:
            file_content = b""
        file_content += content
        self._store.write_bytes(self._object_path(RESUMABLE_DIR, safe_id), file_content)
        size = len(file_content)
        if size >= total_size:
            return file_content[:total_size]
        return None

    def get_file_obj(self, bucket_name, file_name):
        """Gets the meta information for a file within a bucket

        Arguments:
            bucket_name {str} -- Name of the bucket
            file_name {str} -- File name

        Raises:
            NotFound: Raised when the object doesn't exist

        Returns:
            dict -- GCS-like Object resource
        """

        try:
            return self.objects[bucket_name][file_name]
        except KeyError:
            raise NotFound

    def get_resumable_file_obj(self, file_id):
        """Gets the meta information for a file within resumables

        Arguments:
            file_id {str} -- Resumable file id

        Raises:
            NotFound: Raised when the object doesn't exist

        Returns:
            dict -- GCS-like Object resource
        """

        try:
            return self.resumable[file_id]
        except KeyError:
            raise NotFound

    def get_file(self, bucket_name, file_name, show_error=True):
        """Get the raw data of a file within a bucket

        Arguments:
            bucket_name {str} -- Name of the bucket
            file_name {str} -- File name
            show_error {bool} -- Show error if the file is missing

        Raises:
            NotFound: Raised when the object doesn't exist

        Returns:
            bytes -- Raw content of the file
        """

        try:
            return self._store.read_bytes(self._object_path(bucket_name, file_name))
        except (FileNotFoundError, IsADirectoryError, PermissionError) as e:
            if show_error:
                logger.error("Resource not found:")
                logger.error(e)
            raise NotFound

    def delete_resumable_file_obj(self, file_id):
        """Deletes the meta information for a file within resumables

        Arguments:
            file_id {str} -- Resumable file id

        Raises:
            NotFound: Raised when the object doesn't exist
        """

        try:
            del self.resumable[file_id]
        except KeyError:
            raise NotFound

    def delete_bucket(self, bucket_name):
        """Delete a bucket's meta and file

        Arguments:
            bucket_name {str} -- GCS bucket name

        Raises:
            NotFound: If the bucket doesn't exist
            Conflict: If the bucket is not empty or there are pending uploads
        """
        bucket_meta = self.buckets.get(bucket_name)
        if bucket_meta is None:
            raise NotFound("Bucket with name '{}' does not exist".format(bucket_name))

        bucket_objects = self.objects.get(bucket_name, {})

        if len(bucket_objects.keys()) != 0:
            raise Conflict("Bucket '{}' is not empty".format(bucket_name))

        resumable_ids = [
            file_id
            for (file_id, file_obj) in self.resumable.items()
            if file_obj.get("bucket") == bucket_name
        ]

        if len(resumable_ids) != 0:
            raise Conflict(
                "Bucket '{}' has pending upload sessions".format(bucket_name)
            )

        del self.buckets[bucket_name]

        self._delete_dir(bucket_name)
        self._write_config_to_file()

    def delete_file(self, bucket_name, file_name):
        try:
            self.objects[bucket_name][file_name]
        except KeyError:
            raise NotFound(
                "Object with name '{}' does not exist in bucket '{}'".format(
                    bucket_name, file_name
                )
            )

        del self.objects[bucket_name][file_name]

        self._delete_file(bucket_name, file_name)
        self._write_config_to_file()

    def _delete_file(self, bucket_name, file_name):
        try:
            self._store.remove_file(self._object_path(bucket_name, file_name))
        except FileNotFoundError:
            logger.info("No file to remove '{}/{}'".format(bucket_name, file_name))

    def _delete_dir(self, path, force=True):
        try:
            self._store.remove_tree(path)
        except FileNotFoundError:
            logger.info("No folder to remove '{}'".format(path))

    def wipe(self, keep_buckets=False):
        existing_buckets = self.buckets
        self.buckets = {}
        self.objects = {}
        self.resumable = {}

        try:
            self._store.remove_file(".meta")
        except FileNotFoundError:
            pass
        try:
            for name in list(self._store.listdir("")):
                self._store.remove_tree(name)
        except FileNotFoundError as e:
            logger.warning(e)

        if keep_buckets:
            for bucket_name, bucket_obj in existing_buckets.items():
                self.create_bucket(bucket_name, bucket_obj)

    def patch_object(self, bucket_name, file_name, file_obj):
        """Patch object

        Arguments:
            bucket_name {str} -- Name of the bucket to save to
            file_name {str} -- File name used to store data
            file_obj {dict} -- GCS-like Object resource
        """

        bucket_objects = self.objects.get(bucket_name)
        if bucket_objects and bucket_objects.get(file_name):
            bucket_objects[file_name] = file_obj
            self.objects[bucket_name] = bucket_objects
            self._write_config_to_file()

    @staticmethod
    def safe_id(file_id):
        """Safe string from the resumable file_id

         Arguments:
            file_id {str} -- Resumable file id

        Returns:
            str -- Safe string to use in the file system
        """
        return sha256(file_id.encode("utf-8")).hexdigest()
