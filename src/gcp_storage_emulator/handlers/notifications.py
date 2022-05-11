import logging
from gcp_storage_emulator import settings
from http import HTTPStatus


logger = logging.getLogger("api.notification")


def _make_notification_resource(bucket_name, topic_name, topic, payload_format, notification_id):
    return {
        "kind": "storage#notification",
        "selfLink": "{}/b/{}/notificationConfigs/{}".format(settings.API_ENDPOINT, bucket_name, topic_name),
        "id": notification_id,
        "topic": topic,
        "etag": notification_id,
        "payload_format": payload_format,
        "event_types": [
            "OBJECT_FINALIZE",
            "OBJECT_METADATA_UPDATE",
            "OBJECT_ARCHIVE",
            "OBJECT_DELETE"
        ]
    }


def get(request, response, storage, *args, **kwargs):
    # TODO
    response.status = HTTPStatus.NOT_FOUND


def getbyid(request, response, storage, *args, **kwargs):
    # TODO
    response.status = HTTPStatus.NOT_FOUND


def create_notification(bucket_name, topic, payload_format, storage):
    topic_name = topic.split('/')[-1]

    notifications = storage.get_notifications(bucket_name, topic_name)
    notification_id = max(notification.get('id', 0) for notification in notifications) + 1

    notification = _make_notification_resource(bucket_name, topic_name, topic, payload_format, str(notification_id))
    storage.create_notification(bucket_name, topic_name, notification)
    return notification


def insert(request, response, storage, *args, **kwargs):
    bucket_name = request.params.get('bucket_name')

    if bucket_name:
        topic = request.data.get('topic')

        payload_format = request.data.get('payload_format')
        logger.debug(
            "[BUCKETS] Received request to create notification in bucket {}".format(bucket_name)
        )
        notification = create_notification(bucket_name, topic, payload_format, storage)
        response.json(notification)
    else:
        response.status = HTTPStatus.BAD_REQUEST


def delete(request, response, storage, *args, **kwargs):
    # TODO
    response.status = HTTPStatus.NOT_FOUND
