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
    }


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
