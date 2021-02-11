import hashlib
import math
import time
import urllib.parse
from base64 import b64encode
from datetime import datetime, timezone
from http import HTTPStatus

import crc32c
from gcp_storage_emulator.exceptions import Conflict, NotFound


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

BAD_REQUEST = {
    "error": {
        "errors": [
            {
                "domain": "global",
                "reason": "invalid",
                "message": None
            }
        ],
        "code": 400,
        "message": None
    }
}

MD5_CHECKSUM_ERROR = "Provided MD5 hash \"{}\" doesn't match calculated MD5 hash \"{}\"."
CRC32C_CHECKSUM_ERROR = "Provided CRC32C \"{}\" doesn't match calculated CRC32C \"{}\"."


def _handle_conflict(response, err):
    msg = str(err)
    response.status = HTTPStatus.BAD_REQUEST
    resp = BAD_REQUEST
    resp["error"]["message"] = msg
    resp["error"]["errors"][0]["message"] = msg
    response.json(resp)


def _crc32c(content):
    if isinstance(content, str):
        content = content.encode()
    val = crc32c.crc32c(content)
    return b64encode(val.to_bytes(4, byteorder='big')).decode('ascii')


def _md5(content):
    if isinstance(content, str):
        content = content.encode()
    return b64encode(hashlib.md5(content).digest()).decode('ascii')


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
        for key in _WRITABLE_FIELDS:
            val = metadata.get(key)
            if val is not None:
                if key == "customTime" and obj.get(key) and obj.get(key) > val:
                    continue
                obj[key] = val
    return obj


def _make_object_resource(base_url, bucket_name, object_name, content_type, content_length, metadata=None):
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
        "etag": None
    }
    obj = _patch_object(obj, metadata)
    return obj


def _multipart_upload(request, response, storage):
    obj = _make_object_resource(
        request.base_url,
        request.params["bucket_name"],
        request.data["meta"]["name"],
        request.data["content-type"],
        str(len(request.data["content"])),
        request.data["meta"],
    )
    try:
        obj = _checksums(request.data["content"], obj)
        storage.create_file(
            request.params["bucket_name"],
            request.data["meta"]["name"],
            request.data["content"],
            obj,
        )

        response.json(obj)
    except Conflict as err:
        _handle_conflict(response, err)


def _create_resumable_upload(request, response, storage):
    content_type = request.get_header('x-upload-content-type', 'application/octet-stream')
    content_length = request.get_header('x-upload-content-length', None)
    obj = _make_object_resource(
        request.base_url,
        request.params["bucket_name"],
        request.data["name"],
        content_type,
        content_length,
    )

    id = storage.create_resumable_upload(
        request.params["bucket_name"],
        request.data["name"],
        obj,
    )

    encoded_id = urllib.parse.urlencode({
        'upload_id': id,
    })
    response["Location"] = request.full_url + "&{}".format(encoded_id)


def insert(request, response, storage, *args, **kwargs):
    uploadType = request.query.get("uploadType")

    if not uploadType or len(uploadType) == 0:
        response.status = HTTPStatus.BAD_REQUEST
        return

    uploadType = uploadType[0]

    if uploadType == "resumable":
        return _create_resumable_upload(request, response, storage)

    if uploadType == "multipart":
        return _multipart_upload(request, response, storage)


def upload_partial(request, response, storage, *args, **kwargs):
    upload_id = request.query.get("upload_id")[0]
    try:
        obj = storage.get_resumable_file_obj(upload_id)
        obj = _checksums(request.data, obj)
        obj["size"] = str(len(request.data))
        obj = storage.create_file(obj["bucket"], obj["name"], request.data, obj, upload_id)
        return response.json(obj)
    except Conflict as err:
        _handle_conflict(response, err)


def get(request, response, storage, *args, **kwargs):
    try:
        obj = storage.get_file_obj(request.params["bucket_name"], request.params["object_id"])
        response.json(obj)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND


def ls(request, response, storage, *args, **kwargs):
    bucket_name = request.params["bucket_name"]
    prefix = request.query.get("prefix")[0] if request.query.get("prefix") else None
    delimiter = request.query.get('delimiter')[0] if request.query.get("delimiter") else None
    try:
        files = storage.get_file_list(bucket_name, prefix, delimiter)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
    else:
        response.json({
            "kind": "storage#object",
            "items": files
        })


def copy(request, response, storage, *args, **kwargs):
    try:
        obj = storage.get_file_obj(request.params["bucket_name"], request.params["object_id"])
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
        storage.create_file(request.params["dest_bucket_name"], request.params["dest_object_id"], file, dest_obj)
        response.json(dest_obj)
    except Conflict as err:
        _handle_conflict(response, err)


def download(request, response, storage, *args, **kwargs):
    try:
        file = storage.get_file(request.params["bucket_name"], request.params["object_id"])
        obj = storage.get_file_obj(request.params["bucket_name"], request.params["object_id"])
        response.write_file(file, content_type=obj.get("contentType"))
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND


def delete(request, response, storage, *args, **kwargs):
    try:
        storage.delete_file(request.params["bucket_name"], request.params["object_id"])
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND


def patch(request, response, storage, *args, **kwargs):
    try:
        obj = storage.get_file_obj(request.params["bucket_name"], request.params["object_id"])
        obj = _patch_object(obj, request.data)
        storage.patch_object(request.params["bucket_name"], request.params["object_id"], obj)
        response.json(obj)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
