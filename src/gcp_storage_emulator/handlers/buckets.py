import logging
from datetime import datetime, timezone
from http import HTTPStatus

from gcp_storage_emulator import settings
from gcp_storage_emulator.exceptions import Conflict, NotFound

logger = logging.getLogger("api.bucket")

CONFLICT = {
    "error": {
        "errors": [
            {
                "domain": "global",
                "reason": "conflict",
                "message": "You already own this bucket. Please select another name.",
            }
        ],
        "code": 409,
        "message": "You already own this bucket. Please select another name.",
    }
}

BAD_REQUEST = {
    "error": {
        "errors": [
            {"domain": "global", "reason": "invalid", "message": "Empty bucket name"}
        ],
        "code": 400,
        "message": "Empty bucket name",
    }
}


_WRITABLE_BUCKET_FIELDS = (
    "acl",
    "defaultObjectAcl",
    "cors",
    "lifecycle",
    "location",
    "storageClass",
    "versioning",
    "labels",
    "website",
    "billing",
    "iamConfiguration",
    "encryption",
    "logging",
    "retentionPolicy",
)


def _normalize_bucket_acl_entries(bucket_name, entries, kind):
    """Normalize client ACL entries for bucket or default object ACLs."""
    normalized = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        entity = entry.get("entity")
        role = entry.get("role")
        if not entity or not role:
            continue
        item = {
            "kind": kind,
            "entity": entity,
            "role": role,
            "bucket": bucket_name,
        }
        if entry.get("entityId") is not None:
            item["entityId"] = entry["entityId"]
        if entry.get("email") is not None:
            item["email"] = entry["email"]
        if entry.get("domain") is not None:
            item["domain"] = entry["domain"]
        normalized.append(item)
    return normalized


def _make_bucket_resource(bucket_name):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return {
        "kind": "storage#bucket",
        "id": bucket_name,
        "selfLink": "{}/b/{}".format(settings.API_ENDPOINT, bucket_name),
        "projectNumber": "1234",
        "name": bucket_name,
        "timeCreated": now,
        "updated": now,
        "metageneration": "1",
        "iamConfiguration": {
            "bucketPolicyOnly": {"enabled": False},
            "uniformBucketLevelAccess": {"enabled": False},
        },
        "location": "US",
        "locationType": "multi-region",
        "storageClass": "STANDARD",
        "etag": "CAE=",
        "acl": [],
        "defaultObjectAcl": [],
    }


def _patch_bucket(bucket, metadata):
    if not metadata:
        return bucket
    bucket["metageneration"] = str(int(bucket.get("metageneration", "1")) + 1)
    name = bucket.get("name")
    for key in _WRITABLE_BUCKET_FIELDS:
        val = metadata.get(key)
        if val is None:
            continue
        if key == "acl":
            bucket[key] = _normalize_bucket_acl_entries(
                name, val, "storage#bucketAccessControl"
            )
        elif key == "defaultObjectAcl":
            bucket[key] = _normalize_bucket_acl_entries(
                name, val, "storage#objectAccessControl"
            )
        else:
            bucket[key] = val
    return bucket


def get(request, response, storage, *args, **kwargs):
    name = request.params.get("bucket_name")
    if name and storage.buckets.get(name):
        response.json(storage.buckets.get(name))
    else:
        response.status = HTTPStatus.NOT_FOUND


def ls(request, response, storage, *args, **kwargs):
    logger.info("[BUCKETS] List received")
    response.json(
        {
            "kind": "storage#buckets",
            "items": list(storage.buckets.values()),
        }
    )


def create_bucket(name, storage):
    if storage.get_bucket(name):
        return False
    else:
        bucket = _make_bucket_resource(name)
        storage.create_bucket(name, bucket)
        return bucket


def insert(request, response, storage, *args, **kwargs):
    name = request.data.get("name")
    if name:
        logger.debug(
            "[BUCKETS] Received request to create bucket with name {}".format(name)
        )
        bucket = create_bucket(name, storage)
        if not bucket:
            response.status = HTTPStatus.CONFLICT
            response.json(CONFLICT)
        else:
            bucket = _make_bucket_resource(name)
            storage.create_bucket(name, bucket)
            response.json(bucket)
    else:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(BAD_REQUEST)


def delete(request, response, storage, *args, **kwargs):
    name = request.params.get("bucket_name")
    if not name:
        response.status = HTTPStatus.BAD_REQUEST
        return response.json(BAD_REQUEST)

    try:
        storage.delete_bucket(name)
    except NotFound:
        response.status = HTTPStatus.NOT_FOUND
    except Conflict:
        response.status = HTTPStatus.CONFLICT


def patch(request, response, storage, *args, **kwargs):
    """PATCH /storage/v1/b/{bucket} — update metadata including ACLs."""
    name = request.params.get("bucket_name")
    bucket = storage.buckets.get(name) if name else None
    if not bucket:
        response.status = HTTPStatus.NOT_FOUND
        return
    updated = _patch_bucket(bucket, request.data or {})
    storage.create_bucket(name, updated)
    response.json(updated)


def acl_list(request, response, storage, *args, **kwargs):
    """GET /storage/v1/b/{bucket}/acl"""
    name = request.params.get("bucket_name")
    bucket = storage.buckets.get(name) if name else None
    if not bucket:
        response.status = HTTPStatus.NOT_FOUND
        return
    items = _normalize_bucket_acl_entries(
        name, bucket.get("acl") or [], "storage#bucketAccessControl"
    )
    response.json({"kind": "storage#bucketAccessControls", "items": items})


def default_object_acl_list(request, response, storage, *args, **kwargs):
    """GET /storage/v1/b/{bucket}/defaultObjectAcl"""
    name = request.params.get("bucket_name")
    bucket = storage.buckets.get(name) if name else None
    if not bucket:
        response.status = HTTPStatus.NOT_FOUND
        return
    items = _normalize_bucket_acl_entries(
        name, bucket.get("defaultObjectAcl") or [], "storage#objectAccessControl"
    )
    response.json({"kind": "storage#objectAccessControls", "items": items})


def _default_iam_policy(bucket_name):
    """Stub default IAM policy (stored only; not enforced)."""
    return {
        "kind": "storage#policy",
        "resourceId": "projects/_/buckets/{}".format(bucket_name),
        "version": 1,
        "etag": "CAE=",
        "bindings": [],
    }


def _policy_response(bucket_name, policy):
    """Ensure policy response has required GCS fields."""
    out = dict(policy or {})
    out["kind"] = "storage#policy"
    out["resourceId"] = "projects/_/buckets/{}".format(bucket_name)
    if "version" not in out:
        out["version"] = 1
    if "etag" not in out:
        out["etag"] = "CAE="
    if "bindings" not in out:
        out["bindings"] = []
    return out


def get_iam_policy(request, response, storage, *args, **kwargs):
    """GET /storage/v1/b/{bucket}/iam — store/return only, not enforced (#229)."""
    name = request.params.get("bucket_name")
    if not name or name not in storage.buckets:
        response.status = HTTPStatus.NOT_FOUND
        return
    policy = storage.get_bucket_iam_policy(name)
    if policy is None:
        policy = _default_iam_policy(name)
        storage.set_bucket_iam_policy(name, policy)
    # optionsRequestedPolicyVersion is accepted and ignored (no conditions).
    response.json(_policy_response(name, policy))


def set_iam_policy(request, response, storage, *args, **kwargs):
    """PUT /storage/v1/b/{bucket}/iam — store policy JSON only (#229)."""
    name = request.params.get("bucket_name")
    if not name or name not in storage.buckets:
        response.status = HTTPStatus.NOT_FOUND
        return
    body = request.data if isinstance(request.data, dict) else {}
    # Keep client bindings/version/etag; fill in resource identity fields.
    policy = {
        "kind": "storage#policy",
        "resourceId": "projects/_/buckets/{}".format(name),
        "version": body.get("version", 1),
        "etag": body.get("etag") or "CAI=",
        "bindings": body.get("bindings") if body.get("bindings") is not None else [],
    }
    storage.set_bucket_iam_policy(name, policy)
    response.json(_policy_response(name, policy))


def test_iam_permissions(request, response, storage, *args, **kwargs):
    """GET /storage/v1/b/{bucket}/iam/testPermissions — stub: allow all requested."""
    name = request.params.get("bucket_name")
    if not name or name not in storage.buckets:
        response.status = HTTPStatus.NOT_FOUND
        return
    # Query may repeat permissions=...; parse_qs returns a list.
    permissions = request.query.get("permissions") or []
    response.json(
        {
            "kind": "storage#testIamPermissionsResponse",
            "permissions": list(permissions),
        }
    )
