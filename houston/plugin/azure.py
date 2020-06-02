"""Houston Utilities for Microsoft Azure

Recommended usage:

    # end stage and trigger downstream stages

    res = h.end_stage(stage)

    for next_stage in res['next']:

        topic = (your method of getting the topic for the 'next_stage',
                 e.g. stage_parameter, simple conversion function, hard coded map, etc.)

        topic_key = (your method of getting the topic key for the 'next_stage',
                     e.g. environment variable, key vault secret, load from text file, etc.)

        h.event_grid_trigger({"stage": next_stage, "mission_id": mission_id}, topic, topic_key)

"""

from houston.client import Houston
from datetime import datetime, timezone
from azure.eventgrid import EventGridClient
from msrest.authentication import TopicCredentials
from msrest.exceptions import HttpOperationError
import uuid
import time


class AzureHouston(Houston):

    def event_grid_trigger(self, data, topic, topic_key, retry=3):
        """Sends a message to the provided Event Grid topic with the provided data payload.

        :param dict data: content of the message to be sent. Should contain 'stage' and 'mission_id'. Can contain any
                          additional JSON serializable information.
        :param string topic: The host name of the topic, e.g. 'topic1.westus2-1.eventgrid.azure.net'
        :param string topic_key: A 44 character access key for the topic.
        """
        if 'plan' not in data:
            data['plan'] = self.plan['name']

        try:
            publish_event_grid_event(data, topic, topic_key)

        except HttpOperationError as e:
            # retry for azure errors
            if e.response.status_code >= 500:
                if retry > 0:
                    time.sleep(0.5)
                    self.event_grid_trigger(data, topic, topic_key, retry - 1)
            else:
                raise e


def publish_event_grid_event(data, topic_hostname, topic_key):
    """Sends a message to the provided Event Grid topic with the provided data payload.
    :param dict data: content of the message to be sent.
    :param string topic_hostname: The host name of the topic, e.g. 'topic1.westus2-1.eventgrid.azure.net'
    :param string topic_key: A 44 character access key for the topic.
    """
    credentials = TopicCredentials(topic_key)

    event_grid_client = EventGridClient(credentials)

    event_id = str(uuid.uuid4())

    event_grid_client.publish_events(
        topic_hostname,
        events=[{
            'id': event_id,
            'subject': "Houston Stage Trigger",
            'data': data,
            'event_type': 'HoustonStageTrigger',
            'event_time': datetime.now(timezone.utc),
            'data_version': 1
        }]
    )
