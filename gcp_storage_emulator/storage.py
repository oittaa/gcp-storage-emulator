import datetime
import json
import logging

import fs
from fs.errors import FileExpected, ResourceNotFound

from gcp_storage_emulator.exceptions import Conflict, NotFound
from gcp_storage_emulator.settings import STORAGE_BASE, STORAGE_DIR

logger = logging.getLogger(__name__)


class Storage(object):
    def __init__(self, use_memory_fs=False):
        self._use_memory_fs = use_memory_fs
        self._pwd = fs.open_fs(self.get_storage_base())
        try:
            self._fs = self._pwd.makedir(STORAGE_DIR)
        except fs.errors.DirectoryExists:
            self._fs = self._pwd.opendir(STORAGE_DIR)

        self._read_config_from_file()

    def _write_config_to_file(self):
        data = {
            "buckets": self.buckets,
            "objects": self.objects,
            "resumable": self.resumable,
        }

        with self._fs.open(".meta", mode="w") as meta:
            json.dump(data, meta, indent=2)

    def _read_config_from_file(self):
        try:
            with self._fs.open(".meta", mode="r") as meta:
                data = json.load(meta)
                self.buckets = data.get("buckets")
                self.objects = data.get("objects")
                self.resumable = data.get("resumable")
        except ResourceNotFound:
            self.buckets = {}
            self.objects = {}
            self.resumable = {}

    def _get_or_create_dir(self, bucket_name, file_name):
        try:
            bucket_dir = self._fs.makedir(bucket_name)
        except fs.errors.DirectoryExists:
            bucket_dir = self._fs.opendir(bucket_name)

        dir_name = fs.path.dirname(file_name)
        return bucket_dir.makedirs(dir_name, recreate=True)

    def get_storage_base(self):
        """Returns the pyfilesystem-compatible fs path to the storage

        This is the OSFS if using disk storage, or "mem://" otherwise.
        See https://docs.pyfilesystem.org/en/latest/guide.html#opening-filesystems for more info

        Returns:
            string -- The relevant filesystm
        """

        if self._use_memory_fs:
            return "mem://"
        else:
            return STORAGE_BASE

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

        bucket_objects = self.objects.get(bucket_name, {})
        if prefix:
            # TODO: Still need to implement the last part of the doc string above to
            # TODO: populate blobs.prefixes when using a delimiter.
            return list(file_object for file_name, file_object in bucket_objects.items()
                        if file_name.startswith(prefix)
                        and (not delimiter or delimiter not in file_name[len(prefix+delimiter):]))
        else:
            return list(bucket_objects.values())

    def create_bucket(self, bucket_name, bucket_obj):
        """Create a bucket object representation and save it to the current fs

        Arguments:
            bucket_name {str} -- Name of the GCS bucket
            bucket_obj {dict} -- GCS-like Bucket resource

        Returns:
            [type] -- [description]
        """

        self.buckets[bucket_name] = bucket_obj
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
        """

        file_dir = self._get_or_create_dir(bucket_name, file_name)

        base_name = fs.path.basename(file_name)
        with file_dir.open(base_name, mode="wb") as file:
            file.write(content)
            bucket_objects = self.objects.get(bucket_name, {})
            bucket_objects[file_name] = file_obj
            self.objects[bucket_name] = bucket_objects
            if file_id:
                self.delete_resumable_file_obj(file_id)
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

        Returns:
            str -- id of the resumable upload session (`upload_id`)
        """

        file_id = "{}:{}:{}".format(bucket_name, file_name, datetime.datetime.now())
        self.resumable[file_id] = file_obj
        self._write_config_to_file()
        return file_id

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

    def get_file(self, bucket_name, file_name):
        """Get the raw data of a file within a bucket

        Arguments:
            bucket_name {str} -- Name of the bucket
            file_name {str} -- File name

        Raises:
            NotFound: Raised when the object doesn't exist

        Returns:
            bytes -- Raw content of the file
        """

        try:
            bucket_dir = self._fs.opendir(bucket_name)
            return bucket_dir.open(file_name, mode="rb").read()
        except (FileExpected, ResourceNotFound) as e:
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
            if file_obj.get('bucket') == bucket_name
        ]

        if len(resumable_ids) != 0:
            raise Conflict("Bucket '{}' has pending upload sessions".format(bucket_name))

        del self.buckets[bucket_name]

        self._delete_dir(bucket_name)
        self._write_config_to_file()

    def delete_file(self, bucket_name, file_name):
        try:
            self.objects[bucket_name][file_name]
        except KeyError:
            raise NotFound("Object with name '{}' does not exist in bucket '{}'".format(bucket_name, file_name))

        del self.objects[bucket_name][file_name]

        self._delete_file(bucket_name, file_name)
        self._write_config_to_file()

    def _delete_file(self, bucket_name, file_name):
        try:
            with self._fs.opendir(bucket_name) as bucket_dir:
                bucket_dir.remove(file_name)
        except ResourceNotFound:
            logger.info("No file to remove '{}/{}'".format(bucket_name, file_name))

    def _delete_dir(self, path, force=True):
        try:
            remover = self._fs.removetree if force else self._fs.removedir
            remover(path)
        except ResourceNotFound:
            logger.info("No folder to remove '{}'".format(path))

    def wipe(self):
        self.buckets = {}
        self.objects = {}
        self.resumable = {}

        try:
            self._fs.remove('.meta')
            for path in self._fs.listdir('.'):
                self._fs.removetree(path)
        except ResourceNotFound as e:
            logger.warning(e)

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
