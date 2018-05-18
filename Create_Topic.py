import os
import time

# setup for the pub sub client
os.environ['GOOGLE_CLOUD_DISABLE_GRPC'] = 'true'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.expanduser(
    '~') + '/' + '\Desktop\My_pub_sub\google_cloud_personal.json'
if os.environ.get('PUBSUB_EMULATOR_HOST'):
    del os.environ['PUBSUB_EMULATOR_HOST']
from google.cloud import pubsub
# pip install google-cloud-pubsub

pubsub_client = pubsub.Client()

def create_topic(project, topic_name):
    """Create a new Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic_name)

    topic = publisher.create_topic(topic_path)

    print('Topic created: {}'.format(topic))

    while True:
        receive_message_from_pub_sub('pub3', 'sub3', process_order)