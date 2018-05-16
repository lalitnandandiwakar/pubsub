import json
import os

os.environ['GOOGLE_CLOUD_DISABLE_GRPC'] = 'true'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.expanduser('~') + '/' + '\Desktop\My_pub_sub\google_cloud_personal.json'
if os.environ.get('PUBSUB_EMULATOR_HOST'):
    del os.environ['PUBSUB_EMULATOR_HOST']
from google.cloud import pubsub
#pip install google-cloud-pubsub

pubsub_client = pubsub.Client()
payments_topic = pubsub_client.topic('pub3')

def publish_message(data):
    data = json.dumps(data)
    data = data.encode('utf-8')
    print 'before pushing'
    message_id = payments_topic.publish(data)
    print('Message '+repr(message_id)+'published.')


def process_order_aync(data):
    publish_message(data)

process_order_aync({'pub3msg3': 10})


