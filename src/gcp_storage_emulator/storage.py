import datetime
import json
import logging
import os
import shutil
import time
from hashlib import sha256
from pathlib import Path
from typing import Dict, List, Optional, Set

from gcp_storage_emulator.exceptions import BadRequest, Conflict, NotFound
from gcp_storage_emulator.gcs_glob import gcs_glob_match
from gcp_storage_emulator.settings import (
    DEFAULT_PROJECT_NUMBER,
    STORAGE_BASE,
    STORAGE_DIR,
)

# Real buckets can't start with an underscore
RESUMABLE_DIR = "_resumable"
SOFT_DELETE_DIR = "_softdelete"

# Default soft-delete retention (7 days), matching GCS new-bucket default.
DEFAULT_SOFT_DELETE_RETENTION_SECONDS = 7 * 24 * 60 * 60

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
    def __init__(self, use_memory_fs=False, data_dir=None, project_number=None):
        if not data_dir:
            data_dir = STORAGE_BASE
        if not os.path.isabs(data_dir):
            raise ValueError(f"{data_dir!r} must be an absolute path")

        self._data_dir = data_dir
        self._use_memory_fs = use_memory_fs
        # Used when creating new bucket resources (issue #118).
        if project_number is None:
            project_number = DEFAULT_PROJECT_NUMBER
        self.project_number = str(project_number)
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
            "bucket_iam_policies": self.bucket_iam_policies,
            "soft_deleted": self.soft_deleted,
        }
        self._store.write_bytes(".meta", json.dumps(data, indent=2).encode("utf-8"))

    def _read_config_from_file(self):
        try:
            raw = self._store.read_bytes(".meta")
        except FileNotFoundError:
            self.buckets = {}
            self.objects = {}
            self.resumable = {}
            self.bucket_iam_policies = {}
            self.soft_deleted = {}
            return
        data = json.loads(raw.decode("utf-8"))
        self.buckets = data.get("buckets") or {}
        self.objects = data.get("objects") or {}
        self.resumable = data.get("resumable") or {}
        self.bucket_iam_policies = data.get("bucket_iam_policies") or {}
        self.soft_deleted = data.get("soft_deleted") or {}

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

    def _soft_delete_content_path(self, bucket_name, file_name, generation):
        key = sha256(
            "{}:{}:{}".format(bucket_name, file_name, generation).encode("utf-8")
        ).hexdigest()
        return "{}/{}/{}".format(SOFT_DELETE_DIR, bucket_name, key)

    def _bucket_soft_delete_retention_seconds(self, bucket_name):
        bucket = self.buckets.get(bucket_name) or {}
        policy = bucket.get("softDeletePolicy") or {}
        retention = policy.get("retentionDurationSeconds")
        if retention is None:
            return DEFAULT_SOFT_DELETE_RETENTION_SECONDS
        return int(retention)

    def _list_soft_deleted_candidates(self, bucket_name, prefix=None):
        self._purge_expired_soft_deletes(bucket_name)
        candidates = []
        by_name = self.soft_deleted.get(bucket_name, {})
        for file_name, generations in by_name.items():
            if prefix is not None and not file_name.startswith(prefix):
                continue
            for _gen, file_object in generations.items():
                candidates.append((file_name, file_object))
        candidates.sort(key=lambda item: (item[0], int(item[1].get("generation") or 0)))
        return candidates

    def _list_live_candidates(self, bucket_name, prefix=None):
        bucket_objects = self.objects.get(bucket_name, {})
        return [
            (file_name, file_object)
            for file_name, file_object in bucket_objects.items()
            if prefix is None or file_name.startswith(prefix)
        ]

    def _apply_list_delimiter(self, candidates, prefix, delimiter, match_glob):
        prefix_len = len(prefix) if prefix else 0
        objs = []
        prefixes = set()
        for file_name, file_object in candidates:
            rest = file_name[prefix_len:]
            if delimiter in rest:
                head, _sep, _tail = rest.partition(delimiter)
                prefixes.add(file_name[:prefix_len] + head + delimiter)
            else:
                objs.append(file_object)
        if match_glob is not None:
            prefixes = {
                folder for folder in prefixes if gcs_glob_match(match_glob, folder)
            }
        return objs, prefixes

    def get_file_list(
        self,
        bucket_name,
        prefix=None,
        delimiter=None,
        match_glob=None,
        soft_deleted=False,
    ):
        """Lists objects in a bucket with optional prefix, delimiter, and matchGlob.

        matchGlob follows the GCS objects.list glob syntax:
        https://cloud.google.com/storage/docs/json_api/v1/objects/list#list-object-glob

        When matchGlob is set, delimiter must be omitted or ``/``. Matching object
        names are returned in items; with delimiter ``/``, matching object prefixes
        are returned in prefixes.

        When soft_deleted is True, only soft-deleted object generations are listed.
        """

        if bucket_name not in self.buckets:
            raise NotFound

        if match_glob is not None and delimiter is not None and delimiter != "/":
            raise BadRequest(
                "When listing with a glob pattern, the only supported delimiter is '/'."
            )

        if soft_deleted:
            candidates = self._list_soft_deleted_candidates(bucket_name, prefix)
        else:
            candidates = self._list_live_candidates(bucket_name, prefix)

        if match_glob is not None:
            candidates = [
                (file_name, file_object)
                for file_name, file_object in candidates
                if gcs_glob_match(match_glob, file_name)
            ]

        if delimiter:
            objs, prefixes = self._apply_list_delimiter(
                candidates, prefix, delimiter, match_glob
            )
        else:
            objs = [file_object for _name, file_object in candidates]
            prefixes = []

        objs.sort(key=lambda obj: obj.get("name") or "")
        return objs, sorted(prefixes)

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

    def get_resumable_byte_count(self, file_id):
        """Return how many bytes have been received for a resumable upload so far."""
        safe_id = self.safe_id(file_id)
        try:
            return len(self.get_file(RESUMABLE_DIR, safe_id, False))
        except NotFound:
            return 0

    def add_to_resumable_upload(
        self, file_id, content, total_size=None, expected_start=None
    ):
        """Append a chunk to a resumable upload.

        Arguments:
            file_id {str} -- Resumable file id
            content {bytes} -- Chunk content
            total_size {int|None} -- Total object size if known; None if still
                unknown (Content-Range ends with /*)
            expected_start {int|None} -- Expected start offset (must match
                current length when provided)

        Returns:
            bytes -- Full object content if the upload is complete, else None
        """
        safe_id = self.safe_id(file_id)
        try:
            file_content = self.get_file(RESUMABLE_DIR, safe_id, False)
        except NotFound:
            file_content = b""

        if expected_start is not None and expected_start != len(file_content):
            raise BadRequest(
                "Invalid Content-Range start: expected {}, got {}".format(
                    len(file_content), expected_start
                )
            )

        file_content += content or b""
        self._store.write_bytes(self._object_path(RESUMABLE_DIR, safe_id), file_content)
        size = len(file_content)
        # Incomplete while total is unknown, or while we have not received all bytes.
        if total_size is None or size < total_size:
            return None
        return file_content[:total_size]

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
        self.bucket_iam_policies.pop(bucket_name, None)
        self.soft_deleted.pop(bucket_name, None)
        self.objects.pop(bucket_name, None)

        self._delete_dir(bucket_name)
        try:
            self._store.remove_tree("{}/{}".format(SOFT_DELETE_DIR, bucket_name))
        except FileNotFoundError:
            pass
        self._write_config_to_file()

    def get_bucket_iam_policy(self, bucket_name):
        """Return stored IAM policy for a bucket, or None if bucket missing."""
        if bucket_name not in self.buckets:
            return None
        return self.bucket_iam_policies.get(bucket_name)

    def set_bucket_iam_policy(self, bucket_name, policy):
        """Store IAM policy for a bucket. Returns False if bucket missing."""
        if bucket_name not in self.buckets:
            return False
        self.bucket_iam_policies[bucket_name] = policy
        self._write_config_to_file()
        return True

    def _hard_delete_file(self, bucket_name, file_name):
        """Permanently remove a live object (meta + content)."""
        try:
            del self.objects[bucket_name][file_name]
        except KeyError:
            raise NotFound(
                "Object with name '{}' does not exist in bucket '{}'".format(
                    file_name, bucket_name
                )
            )
        self._delete_file(bucket_name, file_name)
        self._write_config_to_file()

    def delete_file(self, bucket_name, file_name):
        """Delete a live object, applying soft delete when the bucket policy allows."""
        if bucket_name not in self.buckets:
            raise NotFound(
                "Object with name '{}' does not exist in bucket '{}'".format(
                    file_name, bucket_name
                )
            )
        try:
            file_obj = self.objects[bucket_name][file_name]
        except KeyError:
            raise NotFound(
                "Object with name '{}' does not exist in bucket '{}'".format(
                    file_name, bucket_name
                )
            )

        retention = self._bucket_soft_delete_retention_seconds(bucket_name)
        if retention <= 0:
            self._hard_delete_file(bucket_name, file_name)
            return

        try:
            content = self.get_file(bucket_name, file_name, show_error=False)
        except NotFound:
            content = b""

        generation = str(file_obj.get("generation") or "0")
        now = datetime.datetime.now(datetime.timezone.utc)
        soft_delete_time = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        hard_delete = now + datetime.timedelta(seconds=retention)
        hard_delete_time = hard_delete.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        soft_obj = dict(file_obj)
        soft_obj["softDeleteTime"] = soft_delete_time
        soft_obj["hardDeleteTime"] = hard_delete_time

        content_path = self._soft_delete_content_path(
            bucket_name, file_name, generation
        )
        self._store.write_bytes(content_path, content or b"")

        by_name = self.soft_deleted.setdefault(bucket_name, {})
        by_name.setdefault(file_name, {})[generation] = soft_obj

        del self.objects[bucket_name][file_name]
        self._delete_file(bucket_name, file_name)
        self._write_config_to_file()

    def get_soft_deleted_file_obj(self, bucket_name, file_name, generation):
        """Return soft-deleted object metadata or raise NotFound."""
        self._purge_expired_soft_deletes(bucket_name)
        generation = str(generation)
        try:
            return self.soft_deleted[bucket_name][file_name][generation]
        except KeyError:
            raise NotFound(
                "Soft-deleted object '{}/{}' generation {} not found".format(
                    bucket_name, file_name, generation
                )
            )

    def get_soft_deleted_file(self, bucket_name, file_name, generation):
        """Return soft-deleted object content bytes."""
        self.get_soft_deleted_file_obj(bucket_name, file_name, generation)
        generation = str(generation)
        path = self._soft_delete_content_path(bucket_name, file_name, generation)
        try:
            return self._store.read_bytes(path)
        except FileNotFoundError:
            return b""

    def restore_soft_deleted_file(self, bucket_name, file_name, generation):
        """Restore a soft-deleted object to a new live generation.

        Per GCS, the soft-deleted copy is retained until its hard delete time.
        If a live object with the same name exists, it is soft-deleted first.
        Returns the new live object resource.
        """
        if bucket_name not in self.buckets:
            raise NotFound

        generation = str(generation)
        soft_obj = self.get_soft_deleted_file_obj(bucket_name, file_name, generation)
        content = self.get_soft_deleted_file(bucket_name, file_name, generation)

        # Soft-delete any current live object with this name.
        if file_name in self.objects.get(bucket_name, {}):
            self.delete_file(bucket_name, file_name)

        now = datetime.datetime.now(datetime.timezone.utc)
        time_id = time.time_ns()
        new_obj = dict(soft_obj)
        new_obj.pop("softDeleteTime", None)
        new_obj.pop("hardDeleteTime", None)
        new_obj["generation"] = str(time_id)
        new_obj["metageneration"] = "1"
        new_obj["timeCreated"] = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        new_obj["updated"] = new_obj["timeCreated"]
        new_obj["id"] = "{}/{}/{}".format(bucket_name, file_name, time_id)
        if "mediaLink" in new_obj and isinstance(new_obj["mediaLink"], str):
            # Point mediaLink at the new generation when possible.
            base = new_obj["mediaLink"].split("?")[0]
            new_obj["mediaLink"] = "{}?generation={}&alt=media".format(base, time_id)

        self.create_file(bucket_name, file_name, content, new_obj)
        return new_obj

    def _parse_hard_delete_time(self, hard):
        if not hard:
            return None
        try:
            hard_dt = datetime.datetime.fromisoformat(hard.replace("Z", "+00:00"))
        except ValueError:
            return None
        if hard_dt.tzinfo is None:
            hard_dt = hard_dt.replace(tzinfo=datetime.timezone.utc)
        return hard_dt

    def _remove_soft_deleted_generation(self, bucket_name, file_name, generation):
        path = self._soft_delete_content_path(bucket_name, file_name, generation)
        try:
            self._store.remove_file(path)
        except FileNotFoundError:
            pass
        by_name = self.soft_deleted.get(bucket_name, {})
        generations = by_name.get(file_name, {})
        generations.pop(str(generation), None)
        if not generations and file_name in by_name:
            del by_name[file_name]
        if bucket_name in self.soft_deleted and not self.soft_deleted[bucket_name]:
            del self.soft_deleted[bucket_name]

    def _purge_expired_soft_deletes(self, bucket_name=None):
        """Permanently remove soft-deleted objects past hardDeleteTime."""
        now = datetime.datetime.now(datetime.timezone.utc)
        buckets = [bucket_name] if bucket_name else list(self.soft_deleted.keys())
        changed = False
        for bkt in buckets:
            by_name = self.soft_deleted.get(bkt, {})
            for file_name in list(by_name.keys()):
                for gen, obj in list(by_name.get(file_name, {}).items()):
                    hard_dt = self._parse_hard_delete_time(obj.get("hardDeleteTime"))
                    if hard_dt is not None and hard_dt <= now:
                        self._remove_soft_deleted_generation(bkt, file_name, gen)
                        changed = True
        if changed:
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
        existing_iam = dict(self.bucket_iam_policies) if keep_buckets else {}
        self.buckets = {}
        self.objects = {}
        self.resumable = {}
        self.bucket_iam_policies = {}
        self.soft_deleted = {}

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
                if bucket_name in existing_iam:
                    self.bucket_iam_policies[bucket_name] = existing_iam[bucket_name]
            self._write_config_to_file()

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
