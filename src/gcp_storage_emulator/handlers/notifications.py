import re
import logging
from http import HTTPStatus
import google.cloud.pubsub as pubsub
from gcp_storage_emulator import settings


logger = logging.getLogger("api.notification")


EMPTY_BUCKET_NAME_ERROR = {
    "error": {
        "code": 400,
        "message": "Empty bucket name",
        "errors": [
            {
                "message": "Empty bucket name",
                "domain": "global",
                "reason": "invalid",
            }
        ]
    }
}

EMPTY_NOTIFICATION_ID_ERROR = {
    "error": {
        "code": 400,
        "message": "Empty notification id",
        "errors": [
            {
                "message": "Empty notification id",
                "domain": "global",
                "reason": "invalid",
            }
        ]
    }
}

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

RESOURCE_NOT_FOUND_ERROR = {
    "error": {
        "code": 404,
        "message": "The requested resource was not found.",
        "errors": [
            {
                "message": "The requested resource was not found.",
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

TOPIC_NOT_FOUND_ERROR = {
    "error": {
        "code": 400,
        "message": "Cloud Pub/Sub topic not found, or user does not have permission to it.'",
        "errors": [
            {
                "message": "Cloud Pub/Sub topic not found, or user does not have permission to it.'",
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


def handle_event_types(event_types):
    if type(event_types) != list:
        event_types = []
    event_types = [event_type for event_type in event_types if event_type in EVENT_TYPES]

    return event_types


def get_topic_configuration(project_id, topic_name):
    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    try:
        return publisher.get_topic(request={"topic": topic_path})
    except Exception:
        return None


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
    bucket_name = request.params.get('bucket_name')
    if not bucket_name:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(EMPTY_BUCKET_NAME_ERROR)

        return

    if not storage.get_bucket(bucket_name):
        response.status = HTTPStatus.NOT_FOUND
        response.json(BUCKET_NOT_FOUND_ERROR)

        return

    json_response = {"kind": "storage#notifications"}

    bucket_notifications = storage.get_notifications(bucket_name)
    if bucket_notifications:
        json_response['items'] = bucket_notifications

    response.json(json_response)


def get(request, response, storage, *args, **kwargs):
    bucket_name = request.params.get('bucket_name')
    if not bucket_name:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(EMPTY_BUCKET_NAME_ERROR)

        return

    if not storage.get_bucket(bucket_name):
        response.status = HTTPStatus.NOT_FOUND
        response.json(BUCKET_NOT_FOUND_ERROR)

        return

    notification_id = request.params.get('notification_id')
    if not notification_id:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(EMPTY_NOTIFICATION_ID_ERROR)

        return

    notifications = storage.get_notifications(bucket_name)

    notification = next((notification for notification in notifications if notification['id'] == notification_id), None)
    if not notification:
        response.status = HTTPStatus.NOT_FOUND
        response.json(RESOURCE_NOT_FOUND_ERROR)

        return

    response.json(notification)


def create_notification(bucket_name, topic, topic_name, payload_format, event_types, storage):
    notifications = storage.get_notifications(bucket_name)
    last_notification_id = max([notification.get('id', 0) for notification in notifications], default="0")

    notification_id = str(int(last_notification_id) + 1)
    notification = _make_notification_resource(bucket_name, topic, topic_name, payload_format, event_types, notification_id)
    storage.create_notification(bucket_name, notification)

    return notification


def insert(request, response, storage, *args, **kwargs):
    bucket_name = request.params.get('bucket_name')
    if not bucket_name:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(EMPTY_BUCKET_NAME_ERROR)

        return

    if not storage.get_bucket(bucket_name):
        response.status = HTTPStatus.NOT_FOUND
        response.json(BUCKET_NOT_FOUND_ERROR)

        return

    topic = request.data.get('topic') if request.data else None
    if not topic:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(MISSING_TOPIC_ERROR)

        return

    topic_pattern = re.compile(TOPIC_REGEX)
    topic_match = topic_pattern.fullmatch(topic)
    if not topic_match:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(INVALID_TOPIC_ERROR)

        return

    payload_format = request.data.get('payload_format')
    if payload_format not in PAYLOAD_FORMATS:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(PAYLOAD_FORMAT_REQUIREMENT_ERROR)

        return

    project_id = topic_match.groupdict().get('project_id')
    topic_name = topic_match.groupdict().get('topic_name')
    if not get_topic_configuration(project_id, topic_name):
        response.status = HTTPStatus.NOT_FOUND
        response.json(TOPIC_NOT_FOUND_ERROR)

        return

    logger.debug("[BUCKETS] Received request to create notification in bucket {}".format(bucket_name))

    event_types = handle_event_types(request.data.get('event_type'))
    notification = create_notification(bucket_name, topic, topic_name, payload_format, event_types, storage)

    response.json(notification)


def delete(request, response, storage, *args, **kwargs):
    bucket_name = request.params.get('bucket_name')
    if not bucket_name:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(EMPTY_BUCKET_NAME_ERROR)

        return

    if not storage.get_bucket(bucket_name):
        response.status = HTTPStatus.NOT_FOUND
        response.json(BUCKET_NOT_FOUND_ERROR)

        return

    notification_id = request.params.get('notification_id')
    if not notification_id:
        response.status = HTTPStatus.BAD_REQUEST
        response.json(EMPTY_NOTIFICATION_ID_ERROR)

        return

    notifications = storage.get_notifications(bucket_name)

    notification = next((notification for notification in notifications if notification['id'] == notification_id), None)
    if not notification:
        response.status = HTTPStatus.NOT_FOUND
        response.json(RESOURCE_NOT_FOUND_ERROR)

        return

    storage.delete_notification(bucket_name, notification)

    response.status = HTTPStatus.NO_CONTENT
