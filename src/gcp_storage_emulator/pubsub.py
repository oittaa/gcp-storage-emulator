from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from datetime import datetime


def send_upload_notification_to_pub_sub(
    project_id: str, topic_name: str, bucket_name: str, notification_id: int, upload_path: str
):
    publisher: PublisherClient = PublisherClient()
    topic_path: str = publisher.topic_path(project_id, topic_name)

    notification_config: str = (
        f'projects/{project_id}/'
        f'buckets/{bucket_name}/'
        f'notificationConfigs/{notification_id}'
    )

    attrs: dict = {
        'eventType': 'OBJECT_FINALIZE',
        'objectId': upload_path,
        'objectGeneration': '1',
        'notificationConfig': notification_config,
        'payloadFormat': 'NONE',
        'eventTime': str(datetime.now()),
        'bucketId': bucket_name
    }

    publish_future = publisher.publish(
        topic_path, ''.encode("utf-8"), **attrs)

    futures.wait([publish_future], return_when=futures.ALL_COMPLETED)
