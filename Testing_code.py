import os
import sys
import logging
import traceback
import json
import time
import argparse
import grequests
import grpc

from item_store import ItemStore

from google.cloud.pubsub_v1.subscriber.policy.thread import Policy
from google.cloud import pubsub_v1
requests = ItemStore()

class OurPolicy(Policy):
    """
    We occasionally see errors that google code doesn't
    recover from, set a flag that let's the outer thread respond
    by restarting the client.
    grpc._channel._Rendezvous: <_Rendezvous of RPC that terminated
    with (StatusCode.UNAVAILABLE, OS Error)>
    """
    _exception_caught = None
    def __init__(self, *args, **kws):
        return super(OurPolicy, self).__init__(*args, **kws)
    def on_exception(self, exc):
        # If this is DEADLINE_EXCEEDED, then we want to retry by returning
        # None instead of raise-ing
        deadline_exceeded = grpc.StatusCode.DEADLINE_EXCEEDED
        code_value = getattr(exc, 'code', lambda: None)()
        if code_value == deadline_exceeded:
            return
        OurPolicy._exception_caught = exc
        # will just raise exc
        return super(OurPolicy, self).on_exception(exc)

class InvalidSchemaException(Exception):
    pass

def log_unhandled_exception(type, value, traceback):
    logger.error(type, value, traceback)

sys.excepthook = log_unhandled_exception

def send_to_data_insertion(message):
    requests.add(grequests.post('http://%s:8080/url_here=%s' % (
        address, token),
        data=message.data.decode('utf-8')))


###############  Subscriber logic here ####################
def receive_messages(project, subscription_name):
    subscriber = pubsub_v1.SubscriberClient(policy_class=OurPolicy)
    subscription_path = subscriber.subscription_path(
        project, subscription_name)

    def callback(message):
        message.ack()
        try:
            send_to_data_insertion(message)

        except InvalidSchemaException as e:
            return
        except Exception as e:
            return

    while live_forever:
        if subscriber is None:
            logger.warning('Starting pubsub subscriber client')
            subscriber = pubsub_v1.SubscriberClient(policy_class=OurPolicy)

        subscriber.subscribe(subscription_path).open(callback=callback)

        try:
            while True:
                grequests.map(requests.getAll())
                time.sleep(sleep_interval)

                if OurPolicy._exception_caught:
                    exc = OurPolicy._exception_caught
                    OurPolicy._exception_caught = None
                    raise exc
        except KeyboardInterrupt:
            break
        except Exception as e:
            subscriber = None

    # otherwise, sleep for one interval and exit
    time.sleep(sleep_interval)

#####################################################


if __name__ == "__main__":
    parser.add_argument('project', help='Google cloud project ID')
    parser.add_argument('subscription', help="Google cloud subscription name")
    args = parser.parse_args()
    receive_messages(args.project, args.subscription)