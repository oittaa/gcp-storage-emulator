import hashlib
import json
import logging
import math
import re
import secrets
import string
import time
import urllib.parse
from base64 import b64encode
from copy import deepcopy
from datetime import datetime, timezone
from enum import IntEnum
from http import HTTPStatus

import google_crc32c

from gcp_storage_emulator.exceptions import Conflict, NotFound

logger = logging.getLogger("api.object")

_WRITABLE_FIELDS = (
    "cacheControl",
    "contentDisposition",
    "contentEncoding",
    "contentLanguage",
    "contentType",
    "crc32c",
    "customTime",
    "md5Hash",
    "metadata",
    "storageClass",
)

_HASH_HEADER = "X-Goog-Hash"

BAD_REQUEST = {
    "error": {
        "errors": [{"domain": "global", "reason": "invalid", "message": None}],
        "code": 400,
        "message": None,
    }
}

NOT_FOUND = {
    "error": {
        "errors": [{"domain": "global", "reason": "notFound", "message": None}],
        "code": 404,
        "message": None,
    }
}


MD5_CHECKSUM_ERROR = 'Provided MD5 hash "{}" doesn\'t match calculated MD5 hash "{}".'
CRC32C_CHECKSUM_ERROR = 'Provided CRC32C "{}" doesn\'t match calculated CRC32C "{}".'


class GoogleHTTPStatus(IntEnum):
    def __new__(cls, value, phrase, description=""):
        obj = int.__new__(cls, value)
        obj._value_ = value

        obj.phrase = phrase
        obj.description = description
        return obj

    RESUME_INCOMPLETE = 308, "Resume Incomplete"


def _handle_conflict(response, err):
    msg = str(err)
    response.status = HTTPStatus.BAD_REQUEST
    resp = deepcopy(BAD_REQUEST)
    resp["error"]["message"] = msg
    resp["error"]["errors"][0]["message"] = msg
    response.json(resp)


def _crc32c(content):
    if isinstance(content, str):
        content = content.encode()
    val = google_crc32c.Checksum(content)
    return b64encode(val.digest()).decode("ascii")


def _md5(content):
    if isinstance(content, str):
        content = content.encode()
    return b64encode(hashlib.md5(content).digest()).decode("ascii")


def _checksums(content, file_obj):
    crc32c_hash = _crc32c(content)
    obj_crc32c = file_obj.get("crc32c")
    md5_hash = _md5(content)
    obj_md5 = file_obj.get("md5Hash")
    if not obj_crc32c:
        file_obj["crc32c"] = crc32c_hash
    else:
        if obj_crc32c != crc32c_hash:
            raise Conflict(CRC32C_CHECKSUM_ERROR.format(obj_crc32c, crc32c_hash))
    if not obj_md5:
        file_obj["md5Hash"] = md5_hash
    else:
        if obj_md5 != md5_hash:
            raise Conflict(MD5_CHECKSUM_ERROR.format(obj_md5, md5_hash))
    if not file_obj.get("etag"):
        file_obj["etag"] = md5_hash
    return file_obj


def _patch_object(obj, metadata):
    if metadata:
        obj["metageneration"] = str(int(obj["metageneration"]) + 1)
        for key in _WRITABLE_FIELDS:
            val = metadata.get(key)
            if val is not None:
                if key == "customTime" and obj.get(key) and obj.get(key) > val:
                    continue
                obj[key] = val
    return obj


def _make_object_resource(
    base_url, bucket_name, object_name, content_type, content_length, metadata=None
):
    time_id = math.floor(time.time())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    obj = {
        "kind": "storage#object",
        "id": "{}/{}/{}".format(bucket_name, object_name, time_id),
        "selfLink": "/storage/v1/b/{}/o/{}".format(bucket_name, object_name),
        "name": object_name,
        "bucket": bucket_name,
        "generation": str(time_id),
        "metageneration": "1",
        "contentType": content_type,
        "timeCreated": now,
        "updated": now,
        "storageClass": "STANDARD",
        "timeStorageClassUpdated": now,
        "size": content_length,
        "md5Hash": None,
        "mediaLink": "{}/download/storage/v1/b/{}/o/{}?generation={}&alt=media".format(
            base_url,
            bucket_name,
            object_name,
            time_id,
        ),
        "crc32c": None,
        "etag": None,
    }
    obj = _patch_object(obj, metadata)
    return obj


def _content_type_from_request(request, default=None):
    if "contentEncoding" in request.query:
        return request.query["contentEncoding"][0]
    return default


def _media_upload(request, response, storage):
    object_id = request.query["name"][0]
    content_type = _content_type_from_request(
        request, request.get_header("content-type")
    )
    obj = _make_object_resource(
        request.base_url,
        request.params["bucket_name"],
        object_id,
        content_type,
        str(len(request.data)),
    )
    obj = _checksums(request.data, obj)
    storage.create_file(
        request.params["bucket_name"],
        object_id,
        request.data,
        obj,
    )

    response.json(obj)


def _multipart_upload(request, response, storage):
    object_id = request.data["meta"].get("name")
    # Overrides the object metadata's name value, if any.
    if "name" in request.query:
        object_id = request.query["name"][0]
    content_type = _content_type_from_request(request, request.data["content-type"])
    obj = _make_object_resource(
        request.base_url,
        request.params["bucket_name"],
        object_id,
        content_type,
        str(len(request.data["content"])),
        request.data["meta"],
    )
    obj = _checksums(request.data["content"], obj)
    storage.create_file(
        request.params["bucket_name"],
        object_id,
        request.data["content"],
        obj,
    )

    response.json(obj)


def _create_resumable_upload(request, response, storage):
    # Workaround for libraries using POST method when they should be using PUT.
    if "upload_id" in request.query:
        return upload_partial(request, response, storage)
    if request.data:
        object_id = request.data.get("name")
    # Overrides the object metadata's name value, if any.
    if "name" in request.query:
        object_id = request.query["name"][0]
    content_type = _content_type_from_request(
        request, request.get_header("x-upload-content-type", "application/octet-stream")
    )
    content_length = request.get_header("x-upload-content-length", None)
    obj = _make_object_resource(
        request.base_url,
        request.params["bucket_name"],
        object_id,
        content_type,
        content_length,
    )
    id = storage.create_resumable_upload(
        request.params["bucket_name"],
        object_id,
        obj,
    )
    encoded_id = urllib.parse.urlencode(
        {
            "upload_id": id,
        }
    )
    response["Location"] = request.full_url + "&{}".format(encoded_id)


def _delete(storage, bucket_name, object_id):
    try:
        storage.delete_file(bucket_name, object_id)
        return True
    except NotFound:
        return False


def _patch(storage, bucket_name, object_id, metadata):
    try:
        obj = storage.get_file_obj(bucket_name, object_id)
        obj = _patch_object(obj, metadata)
        storage.patch_object(bucket_name, object_id, obj)
        return obj
    except NotFound:
        logger.error(
            "Could not patch {}/{}: with {}".format(bucket_name, object_id, metadata)
        )
        return None


def xml_upload(request, response, storage, *args, **kwargs):
    content_type = request.get_header("Content-Type", "application/octet-stream")
    obj = _make_object_resource(
        request.base_url,
        request.params["bucket_name"],
        request.params["object_id"],
        content_type,
        str(len(request.data)),
    )
    try:
        obj = _checksums(request.data, obj)
        storage.create_file(
            request.params["bucket_name"],
            request.params["object_id"],
            request.data,
            obj,
        )

    except NotFound:
        response.status = HTTPStatus.NOT_FOUND


def insert(request, response, storage, *args, **kwargs):
    uploadType = request.query.get("uploadType")

    if not uploadType or len(uploadType) == 0:
        response.status = HTTPStatus.BAD_REQUEST
        return

    uploadType = uploadType[0]

    try:
        if uploadType == "media":
            return _media_upload(request, response, storage)

        if uploadType == "resumable":
            return _create_resumable_upload(request, response, storage)

        if uploadType == "multipart":
            return _multipart_upload(request, response, storage)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
    except Conflict as err:
        _handle_conflict(response, err)


def upload_partial(request, response, storage, *args, **kwargs):
    """https://cloud.google.com/storage/docs/performing-resumable-uploads"""
    upload_id = request.query.get("upload_id")[0]
    regex = r"^\s*bytes (?P<start>[0-9]+)-(?P<end>[0-9]+)/(?P<total_size>[0-9]+)$"
    pattern = re.compile(regex)
    content_range = request.get_header("Content-Range", "")
    match = pattern.fullmatch(content_range)
    try:
        obj = storage.get_resumable_file_obj(upload_id)
        if match:
            m_dict = match.groupdict()
            total_size = int(m_dict["total_size"])
            data = storage.add_to_resumable_upload(upload_id, request.data, total_size)
            if data is None:
                response.status = GoogleHTTPStatus.RESUME_INCOMPLETE
                response["Range"] = "bytes=0-{}".format(m_dict["end"])
                return
        else:
            data = request.data or b""

        obj = _checksums(data, obj)
        obj["size"] = str(len(data))
        storage.create_file(obj["bucket"], obj["name"], data, obj, upload_id)
        response.json(obj)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
    except Conflict as err:
        _handle_conflict(response, err)


def get(request, response, storage, *args, **kwargs):
    if request.query.get("alt") and request.query.get("alt")[0] == "media":
        return download(request, response, storage)
    try:
        obj = storage.get_file_obj(
            request.params["bucket_name"], request.params["object_id"]
        )
        response.json(obj)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND


def ls(request, response, storage, *args, **kwargs):
    bucket_name = request.params["bucket_name"]
    prefix = request.query.get("prefix")[0] if request.query.get("prefix") else None
    delimiter = (
        request.query.get("delimiter")[0] if request.query.get("delimiter") else None
    )
    try:
        files, prefixes = storage.get_file_list(bucket_name, prefix, delimiter)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
    else:
        response.json({"kind": "storage#object", "prefixes": prefixes, "items": files})


def copy(request, response, storage, *args, **kwargs):
    try:
        obj = storage.get_file_obj(
            request.params["bucket_name"], request.params["object_id"]
        )
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
        return

    dest_obj = _make_object_resource(
        request.base_url,
        request.params["dest_bucket_name"],
        request.params["dest_object_id"],
        obj["contentType"],
        obj["size"],
        obj,
    )

    file = storage.get_file(request.params["bucket_name"], request.params["object_id"])
    try:
        dest_obj = _checksums(file, dest_obj)
        storage.create_file(
            request.params["dest_bucket_name"],
            request.params["dest_object_id"],
            file,
            dest_obj,
        )
        response.json(dest_obj)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
    except Conflict as err:
        _handle_conflict(response, err)


def compose(request, response, storage, *args, **kwargs):
    content_type = None
    dest_file = b""
    try:
        dest_properties = request.data["destination"]
        for src_obj in request.data["sourceObjects"]:
            if content_type is None:
                temp = storage.get_file_obj(
                    request.params["bucket_name"], src_obj["name"]
                )
                content_type = temp["contentType"]
            dest_file += storage.get_file(
                request.params["bucket_name"], src_obj["name"]
            )

    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
        return

    dest_obj = _make_object_resource(
        request.base_url,
        request.params["bucket_name"],
        request.params["object_id"],
        content_type,
        len(dest_file),
        dest_properties,
    )

    try:
        dest_obj = _checksums(dest_file, dest_obj)
        storage.create_file(
            request.params["bucket_name"],
            request.params["object_id"],
            dest_file,
            dest_obj,
        )
        response.json(dest_obj)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
    except Conflict as err:
        _handle_conflict(response, err)


def download(request, response, storage, *args, **kwargs):
    try:
        file = storage.get_file(
            request.params["bucket_name"], request.params["object_id"]
        )
        obj = storage.get_file_obj(
            request.params["bucket_name"], request.params["object_id"]
        )
        range = request.get_header("range", None)
        if range:
            regex = r"^\s*bytes=(?P<start>[0-9]+)-(?P<end>[0-9]*)$"
            pattern = re.compile(regex)
            match = pattern.fullmatch(range)
            if match:
                end = orig_len = len(file)
                m_dict = match.groupdict()
                start = int(m_dict["start"])
                if m_dict["end"]:
                    end = min(orig_len, int(m_dict["end"]) + 1)
                file = file[start:end]
                end -= 1
                response["Content-Range"] = "bytes {}-{}/{}".format(
                    start, end, orig_len
                )
                response.status = HTTPStatus.PARTIAL_CONTENT
        else:
            hash_header = "crc32c={},md5={}".format(obj["crc32c"], obj["md5Hash"])
            response[_HASH_HEADER] = hash_header

        response.write_file(file, content_type=obj.get("contentType"))
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND


def delete(request, response, storage, *args, **kwargs):
    if not _delete(storage, request.params["bucket_name"], request.params["object_id"]):
        response.status = HTTPStatus.NOT_FOUND


def patch(request, response, storage, *args, **kwargs):
    obj = _patch(
        storage,
        request.params["bucket_name"],
        request.params["object_id"],
        request.data,
    )
    if obj:
        response.json(obj)
    else:
        response.status = HTTPStatus.NOT_FOUND


def batch(request, response, storage, *args, **kwargs):
    boundary = "batch_" + "".join(
        secrets.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(32)
    )
    response["Content-Type"] = "multipart/mixed; boundary={}".format(boundary)
    for item in request.data:
        resp_data = None
        response.write("--{}\r\nContent-Type: application/http\r\n".format(boundary))
        method = item.get("method")
        bucket_name = item.get("bucket_name")
        object_id = item.get("object_id")
        meta = item.get("meta")
        if method == "PATCH":
            resp_data = _patch(storage, bucket_name, object_id, meta)
            if resp_data:
                response.write("HTTP/1.1 200 OK\r\n")
                response.write("Content-Type: application/json; charset=UTF-8\r\n")
                response.write(json.dumps(resp_data))
                response.write("\r\n\r\n")
        if method == "DELETE":
            if object_id:
                resp_data = _delete(storage, bucket_name, object_id)
            else:
                try:
                    storage.delete_bucket(bucket_name)
                    resp_data = True
                except (Conflict, NotFound):
                    pass
            if resp_data:
                response.write("HTTP/1.1 204 No Content\r\n")
                response.write("Content-Type: application/json; charset=UTF-8\r\n")
        if not resp_data:
            msg = "No such object: {}/{}".format(bucket_name, object_id)
            resp_data = deepcopy(NOT_FOUND)
            resp_data["error"]["message"] = msg
            resp_data["error"]["errors"][0]["message"] = msg
            response.write("HTTP/1.1 404 Not Found\r\n")
            response.write("Content-Type: application/json; charset=UTF-8\r\n\r\n")
            response.write(json.dumps(resp_data))
            response.write("\r\n\r\n")

    response.write("--{}--".format(boundary))
