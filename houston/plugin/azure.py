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
from houston.exceptions import HoustonServerError, HoustonServerBusy
from datetime import datetime, timezone
from azure.eventgrid import EventGridClient
from msrest.authentication import TopicCredentials
from msrest.exceptions import HttpOperationError
from retry import retry
import uuid


retry_wrapper = retry((HoustonServerError, HoustonServerBusy, OSError, HttpOperationError), tries=3, delay=1, backoff=2)


@retry_wrapper
def event_grid_trigger(client, data):
    if 'plan' not in data:
        data['plan'] = client.plan['name']

    # TODO: get topic from key vault

    stage = data.get('stage')
    if stage is None:
        raise ValueError("Cannot trigger stage: A stage name was not provided.")

    service = client.get_service_from_stage_name(stage)
    if service is None:
        raise ValueError(f"Cannot trigger stage '{stage}': no service is defined for this stage. "
                         f"See https://github.com/datasparq-ai/houston/blob/main/docs/services.md.")

    trigger = service.get('trigger')
    topic = trigger.get('topic')
    topic_key = trigger.get('topic_key')

    publish_event_grid_event(data, topic, topic_key)


class AzureHouston(Houston):

    def event_grid_trigger(self, data):
        """Sends a message to the provided Event Grid topic with the provided data payload.
        Full documentation https://github.com/datasparq-ai/houston/blob/main/docs/services.md#trigger-methods

        :param dict data: content of the message to be sent. Should contain 'stage' and 'mission_id'. Can contain any
                          additional JSON serializable information.
        """
        event_grid_trigger(self, data)


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
