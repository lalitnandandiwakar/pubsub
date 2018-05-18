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


def receive_message_from_pub_sub(topic_name, subscription_name, callback):
    topic = pubsub_client.topic(topic_name)
    subscription = topic.subscription(subscription_name)
    results = subscription.pull(return_immediately=True)
    print('Received {} messages.'.format(len(results)))
    for ack_id, message in results:
        print('* {}: {}, {}'.format(
            message.message_id, message.data, message.attributes))
        callback(message)
        print 'processed the message ' + repr(message.message_id)

    if results:
        subscription.acknowledge([ack_id for ack_id, message in results])


def process_order(data):
    time.sleep(6)


while True:
    receive_message_from_pub_sub('pub1', 'sub22', process_order)