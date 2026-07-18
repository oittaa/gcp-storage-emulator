import hashlib
import json
import logging
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

from gcp_storage_emulator.exceptions import BadRequest, Conflict, NotFound

logger = logging.getLogger("api.object")

_WRITABLE_FIELDS = (
    "acl",
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
                if key == "acl":
                    obj[key] = _normalize_object_acl_entries(
                        obj.get("bucket"), obj.get("name"), val
                    )
                else:
                    obj[key] = val
    return obj


def _make_object_resource(
    base_url, bucket_name, object_name, content_type, content_length, metadata=None
):
    time_id = time.time_ns()
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
        # Object ACLs (used by blob.make_public / make_private).
        "acl": [],
    }
    obj = _patch_object(obj, metadata)
    return obj


def _normalize_object_acl_entries(bucket_name, object_id, entries):
    """Normalize client ACL entries to GCS objectAccessControl resources."""
    normalized = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        entity = entry.get("entity")
        role = entry.get("role")
        if not entity or not role:
            continue
        item = {
            "kind": "storage#objectAccessControl",
            "entity": entity,
            "role": role,
            "bucket": bucket_name,
            "object": object_id,
        }
        if entry.get("entityId") is not None:
            item["entityId"] = entry["entityId"]
        if entry.get("email") is not None:
            item["email"] = entry["email"]
        if entry.get("domain") is not None:
            item["domain"] = entry["domain"]
        normalized.append(item)
    return normalized


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
        metadata={**(request.data or {}), "name": object_id},
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


def _parse_resumable_content_range(content_range):
    """Parse Content-Range used by Python and Node resumable clients.

    Supported forms:
      bytes START-END/TOTAL
      bytes START-END/*
      bytes START-*/TOTAL   (Node single-stream style)
      bytes START-*/*
      bytes */TOTAL         (status / empty completion)
      bytes */*             (status query)
    """
    content_range = (content_range or "").strip()
    patterns = (
        (
            r"bytes (?P<start>[0-9]+)-(?P<end>[0-9]+)/(?P<total>[0-9]+)",
            "chunk",
        ),
        (
            r"bytes (?P<start>[0-9]+)-(?P<end>[0-9]+)/\*",
            "chunk",
        ),
        (
            r"bytes (?P<start>[0-9]+)-\*/(?P<total>[0-9]+)",
            "star_end",
        ),
        (
            r"bytes (?P<start>[0-9]+)-\*/\*",
            "star_end",
        ),
        (
            r"bytes \*/(?P<total>[0-9]+)",
            "status",
        ),
        (
            r"bytes \*/\*",
            "status",
        ),
    )
    for regex, kind in patterns:
        match = re.fullmatch(regex, content_range)
        if not match:
            continue
        groups = match.groupdict()
        start = int(groups["start"]) if "start" in groups else None
        end = int(groups["end"]) if groups.get("end") is not None else None
        total = int(groups["total"]) if groups.get("total") is not None else None
        return {"kind": kind, "start": start, "end": end, "total": total}
    return None


def _resumable_incomplete(response, storage, upload_id, fallback_end=0):
    response.status = GoogleHTTPStatus.RESUME_INCOMPLETE
    received = storage.get_resumable_byte_count(upload_id)
    if received > 0:
        response["Range"] = "bytes=0-{}".format(received - 1)
    else:
        response["Range"] = "bytes=0-{}".format(fallback_end)


def _finalize_resumable_object(response, storage, obj, data, upload_id):
    obj = _checksums(data, obj)
    obj["size"] = str(len(data))
    storage.create_file(obj["bucket"], obj["name"], data, obj, upload_id)
    response.json(obj)


def _handle_resumable_status(response, storage, upload_id, obj, total):
    received = storage.get_resumable_byte_count(upload_id)
    # total == 0 is a valid empty object (Content-Range: bytes */0).
    if total is not None and received >= total >= 0:
        data = storage.add_to_resumable_upload(
            upload_id, b"", total_size=total, expected_start=received
        )
        if data is not None:
            _finalize_resumable_object(response, storage, obj, data, upload_id)
            return
    _resumable_incomplete(response, storage, upload_id)


def _handle_resumable_chunk(response, storage, upload_id, chunk, parsed):
    start = parsed["start"]
    end = parsed["end"]
    total_size = parsed["total"]
    # Node single-stream: bytes START-*/TOTAL — body is the remainder.
    if end is None:
        end = start + len(chunk) - 1 if chunk else start - 1
        if total_size is None:
            total_size = start + len(chunk)

    data = storage.add_to_resumable_upload(
        upload_id,
        chunk,
        total_size=total_size,
        expected_start=start,
    )
    if data is None:
        _resumable_incomplete(response, storage, upload_id, fallback_end=max(end, 0))
        return None
    return data


def upload_partial(request, response, storage, *args, **kwargs):
    """Handle resumable upload chunks.

    https://cloud.google.com/storage/docs/performing-resumable-uploads

    Incomplete chunks must respond with 308 and ``Range: bytes=0-LAST``.
    """
    upload_id = request.query.get("upload_id")[0]
    parsed = _parse_resumable_content_range(
        request.get_header("Content-Range", "") or ""
    )
    try:
        obj = storage.get_resumable_file_obj(upload_id)
        chunk = request.data or b""

        if parsed is None:
            data = chunk
        elif parsed["kind"] == "status":
            _handle_resumable_status(response, storage, upload_id, obj, parsed["total"])
            return
        else:
            data = _handle_resumable_chunk(response, storage, upload_id, chunk, parsed)
            if data is None:
                return

        _finalize_resumable_object(response, storage, obj, data, upload_id)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
    except Conflict as err:
        _handle_conflict(response, err)
    except BadRequest as err:
        response.status = HTTPStatus.BAD_REQUEST
        response.json({"error": {"message": str(err)}})


def get(request, response, storage, *args, **kwargs):
    if request.query.get("alt") and request.query.get("alt")[0] == "media":
        return download(request, response, storage)
    soft_deleted = False
    if request.query.get("softDeleted"):
        soft_deleted = request.query.get("softDeleted")[0].lower() in (
            "1",
            "true",
            "yes",
        )
    try:
        if soft_deleted:
            generation = None
            if request.query.get("generation"):
                generation = request.query.get("generation")[0]
            if generation is None:
                response.status = HTTPStatus.BAD_REQUEST
                response.json(
                    {
                        "error": {
                            "code": 400,
                            "message": "generation is required when softDeleted=true",
                        }
                    }
                )
                return
            obj = storage.get_soft_deleted_file_obj(
                request.params["bucket_name"],
                request.params["object_id"],
                generation,
            )
        else:
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
    match_glob = (
        request.query.get("matchGlob")[0] if request.query.get("matchGlob") else None
    )
    soft_deleted = False
    if request.query.get("softDeleted"):
        soft_deleted = request.query.get("softDeleted")[0].lower() in (
            "1",
            "true",
            "yes",
        )
    try:
        files, prefixes = storage.get_file_list(
            bucket_name,
            prefix,
            delimiter,
            match_glob,
            soft_deleted=soft_deleted,
        )
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
    except BadRequest:
        response.status = HTTPStatus.BAD_REQUEST
    else:
        response.json({"kind": "storage#objects", "prefixes": prefixes, "items": files})


def _copy(base_url, storage, bucket_name, object_id, dest_bucket_name, dest_object_id):
    """Copy an object. Returns the destination object resource.

    Raises:
        NotFound: If the source object or destination bucket is missing.
        Conflict: If checksum validation fails.
    """
    obj = storage.get_file_obj(bucket_name, object_id)
    dest_obj = _make_object_resource(
        base_url,
        dest_bucket_name,
        dest_object_id,
        obj["contentType"],
        obj["size"],
        obj,
    )
    file = storage.get_file(bucket_name, object_id)
    dest_obj = _checksums(file, dest_obj)
    storage.create_file(
        dest_bucket_name,
        dest_object_id,
        file,
        dest_obj,
    )
    return dest_obj


def copy(request, response, storage, *args, **kwargs):
    try:
        dest_obj = _copy(
            request.base_url,
            storage,
            request.params["bucket_name"],
            request.params["object_id"],
            request.params["dest_bucket_name"],
            request.params["dest_object_id"],
        )
        response.json(dest_obj)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
    except Conflict as err:
        _handle_conflict(response, err)


def rewrite(request, response, storage, *args, **kwargs):
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
        # Official rewrite response schema:
        # https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite
        size = str(dest_obj["size"])
        response.json(
            {
                "kind": "storage#rewriteResponse",
                "totalBytesRewritten": size,
                "objectSize": size,
                "done": True,
                "resource": dest_obj,
            }
        )
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

        if "response-content-disposition" in request.query:
            response["Content-Disposition"] = request.query[
                "response-content-disposition"
            ][0]

        # Custom metadata is returned as x-goog-meta-* response headers on GET/HEAD.
        # https://cloud.google.com/storage/docs/metadata#custom-metadata
        # https://cloud.google.com/storage/docs/xml-api/reference-headers#xgoogmeta
        custom_metadata = obj.get("metadata") or {}
        if isinstance(custom_metadata, dict):
            for key, value in custom_metadata.items():
                if value is None:
                    continue
                response["x-goog-meta-{}".format(key)] = str(value)

        response.write_file(file, content_type=obj.get("contentType"))
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND


def delete(request, response, storage, *args, **kwargs):
    if not _delete(storage, request.params["bucket_name"], request.params["object_id"]):
        response.status = HTTPStatus.NOT_FOUND


def restore(request, response, storage, *args, **kwargs):
    """POST .../o/{object}/restore?generation=... — restore a soft-deleted object."""
    bucket_name = request.params["bucket_name"]
    object_id = request.params["object_id"]
    generation = None
    if request.query.get("generation"):
        generation = request.query.get("generation")[0]
    if generation is None:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(
            {
                "error": {
                    "code": 400,
                    "message": "generation query parameter is required",
                }
            }
        )
        return
    try:
        obj = storage.restore_soft_deleted_file(bucket_name, object_id, generation)
        response.json(obj)
    except NotFound:
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


def acl_list(request, response, storage, *args, **kwargs):
    """List object access controls.

    GET /storage/v1/b/{bucket}/o/{object}/acl
    https://cloud.google.com/storage/docs/json_api/v1/objectAccessControls/list
    """
    bucket_name = request.params["bucket_name"]
    object_id = request.params["object_id"]
    try:
        obj = storage.get_file_obj(bucket_name, object_id)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
        return
    items = _normalize_object_acl_entries(bucket_name, object_id, obj.get("acl") or [])
    response.json(
        {
            "kind": "storage#objectAccessControls",
            "items": items,
        }
    )


def _batch_write_json(response, status_line, payload):
    response.write(status_line + "\r\n")
    response.write("Content-Type: application/json; charset=UTF-8\r\n\r\n")
    response.write(json.dumps(payload))
    response.write("\r\n\r\n")


def _batch_write_not_found(response, bucket_name, object_id):
    msg = "No such object: {}/{}".format(bucket_name, object_id)
    resp_data = deepcopy(NOT_FOUND)
    resp_data["error"]["message"] = msg
    resp_data["error"]["errors"][0]["message"] = msg
    _batch_write_json(response, "HTTP/1.1 404 Not Found", resp_data)


def _batch_write_bad_request(response, err):
    msg = str(err)
    resp_data = deepcopy(BAD_REQUEST)
    resp_data["error"]["message"] = msg
    resp_data["error"]["errors"][0]["message"] = msg
    _batch_write_json(response, "HTTP/1.1 400 Bad Request", resp_data)


def _batch_patch(request, item, storage, bucket_name, object_id, meta, response):
    resp_data = _patch(storage, bucket_name, object_id, meta)
    if not resp_data:
        return False
    _batch_write_json(response, "HTTP/1.1 200 OK", resp_data)
    return True


def _batch_delete(request, item, storage, bucket_name, object_id, meta, response):
    if object_id:
        ok = _delete(storage, bucket_name, object_id)
    else:
        try:
            storage.delete_bucket(bucket_name)
            ok = True
        except (Conflict, NotFound):
            ok = False
    if not ok:
        return False
    response.write("HTTP/1.1 204 No Content\r\n")
    response.write("Content-Type: application/json; charset=UTF-8\r\n\r\n")
    return True


def _batch_post(request, item, storage, bucket_name, object_id, meta, response):
    """Handle batched POST. Only objects.copy (copyTo) is supported here."""
    dest_bucket_name = item.get("dest_bucket_name")
    dest_object_id = item.get("dest_object_id")
    if not object_id or not dest_bucket_name or not dest_object_id:
        return False
    try:
        dest_obj = _copy(
            request.base_url,
            storage,
            bucket_name,
            object_id,
            dest_bucket_name,
            dest_object_id,
        )
    except NotFound:
        return False
    except Conflict as err:
        _batch_write_bad_request(response, err)
        return True
    _batch_write_json(response, "HTTP/1.1 200 OK", dest_obj)
    return True


_BATCH_METHOD_HANDLERS = {
    "PATCH": _batch_patch,
    "DELETE": _batch_delete,
    "POST": _batch_post,
}


def batch(request, response, storage, *args, **kwargs):
    boundary = "batch_" + "".join(
        secrets.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(32)
    )
    response["Content-Type"] = "multipart/mixed; boundary={}".format(boundary)
    for item in request.data:
        response.write("--{}\r\nContent-Type: application/http\r\n".format(boundary))
        method = item.get("method")
        bucket_name = item.get("bucket_name")
        object_id = item.get("object_id")
        meta = item.get("meta")
        handler = _BATCH_METHOD_HANDLERS.get(method)
        handled = False
        if handler is not None:
            handled = handler(
                request, item, storage, bucket_name, object_id, meta, response
            )
        if not handled:
            _batch_write_not_found(response, bucket_name, object_id)

    response.write("--{}--".format(boundary))
