import re
import logging
from http import HTTPStatus
from gcp_storage_emulator import settings


logger = logging.getLogger("api.notification")


BUCKET_NOT_FOUND_ERROR = {
    "error": {
        "code": 404,
        "message": "The specified bucket does not exist.",
        "errors": [
            {
                "message": "The specified bucket does not exist.",
                "domain": "global",
                "reason": "notFound"
            }
        ]
    }
}

PAYLOAD_FORMAT_REQUIREMENT_ERROR = {
    "error": {
        "code": 400,
        "message": "You must specify a payload format in the 'payload_format' field.",
        "errors": [
            {
                "message": "You must specify a payload format in the 'payload_format' field.",
                "domain": "global",
                "reason": "required"
            }
        ]
    }
}

MISSING_TOPIC_ERROR = {
    "error": {
        "code": 400,
        "message": "You must specify a Cloud Pub/Sub topic in the 'topic' field.",
        "errors": [
            {
                "message": "You must specify a Cloud Pub/Sub topic in the 'topic' field.",
                "domain": "global",
                "reason": "required"
            }
        ]
    }
}

INVALID_TOPIC_ERROR = {
    "error": {
        "code": 400,
        "message": "Invalid Google Cloud Pub/Sub topic. \
            It should look like '//pubsub.googleapis.com/projects/*/topics/*.'",
        "errors": [
            {
                "message": "Invalid Google Cloud Pub/Sub topic. \
                    It should look like '//pubsub.googleapis.com/projects/*/topics/*.'",
                "domain": "global",
                "reason": "invalid"
            }
        ]
    }
}

PAYLOAD_FORMATS = [
    "NONE",
    "JSON_API_V1"
]

EVENT_TYPES = [
    "OBJECT_FINALIZE",
    "OBJECT_METADATA_UPDATE",
    "OBJECT_ARCHIVE",
    "OBJECT_DELETE"
]

TOPIC_REGEX = r"//pubsub.googleapis.com/projects/(?P<project_id>.*[^/]+)/topics/(?P<topic_name>.*[^/]+)"


def _make_notification_resource(bucket_name, topic, topic_name, payload_format, event_types, notification_id):
    notification_resource = {
        "kind": "storage#notification",
        "selfLink": "{}/b/{}/notificationConfigs/{}".format(settings.API_ENDPOINT, bucket_name, topic_name),
        "id": notification_id,
        "topic": topic,
        "etag": notification_id,
        "payload_format": payload_format,
    }

    if event_types:
        notification_resource['event_types'] = event_types

    return notification_resource


def ls(request, response, storage, *args, **kwargs):
    # TODO
    response.status = HTTPStatus.NOT_FOUND


def get(request, response, storage, *args, **kwargs):
    # TODO
    response.status = HTTPStatus.NOT_FOUND


def create_notification(bucket_name, topic, topic_name, payload_format, event_types, storage):
    notifications = storage.get_notifications(bucket_name, topic_name)
    last_notification_id = max([notification.get('id', 0) for notification in notifications], default="0")
    notification_id = str(int(last_notification_id) + 1)

    notification = _make_notification_resource(bucket_name, topic, topic_name, payload_format, event_types, notification_id)
    storage.create_notification(bucket_name, topic_name, notification)
    return notification


def insert(request, response, storage, *args, **kwargs):
    bucket_name = request.params.get('bucket_name')
    if not bucket_name:
        response.status = HTTPStatus.NOT_FOUND
        response.json(BUCKET_NOT_FOUND_ERROR)

        return

    topic = request.data.get('topic')
    if not topic:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(MISSING_TOPIC_ERROR)

        return

    topic_name_pattern = re.compile(TOPIC_REGEX)
    topic_name_match = topic_name_pattern.fullmatch(topic)
    if not topic_name_match:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(INVALID_TOPIC_ERROR)

        return

    topic_name = topic_name_match.groupdict().get('topic_name')

    payload_format = request.data.get('payload_format')
    if payload_format not in PAYLOAD_FORMATS:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(PAYLOAD_FORMAT_REQUIREMENT_ERROR)

        return

    # TODO Validate topic name existence

    event_types = request.data.get('event_type')
    if type(event_types) != list:
        event_types = []
    event_types = [event_type for event_type in event_types if event_type in EVENT_TYPES]

    logger.debug("[BUCKETS] Received request to create notification in bucket {}".format(bucket_name))

    notification = create_notification(bucket_name, topic, topic_name, payload_format, event_types, storage)

    response.json(notification)


def delete(request, response, storage, *args, **kwargs):
    # TODO
    response.status = HTTPStatus.NOT_FOUND
