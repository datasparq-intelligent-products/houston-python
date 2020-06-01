"""Houston Utilities for Google Cloud Platform

PubSub utils:
Allows user to create Google Cloud Pub/Sub message according to the plan stage options, e.g.:

    h.project = "my-project-1234"  # set the Google Cloud project name in the client

    res = h.end_stage("load-data", mission_id)

    for next_stage in res['next']:

        h.pubsub_trigger({'stage': next_stage, 'mission_id': mission_id}, topic=h.get_params(next_stage)['topic'])

--> sends a Pub/Sub message to the next tasks' topics. This assumes we have given each stage a 'topic' parameter.

Note: The topic name can either be provided as an argument or can be set as a parameter for the stage as 'topic' or
'psq', in which case it will be found automatically.

or:

    h.project = "my-project-1234"  # set the Google Cloud project name in the client

    response = h.end_stage("load-data", mission_id)

    h.call_stage_via_pubsub(response, mission_id)  # assumes each stage has a 'psq' parameter which gives the topic name

"""

import base64
import json
import os
from google.cloud import pubsub_v1
from houston.client import Houston


class GCPHouston(Houston):

    project = os.getenv("GCP_PROJECT", None)
    topic = None

    def pubsub_trigger(self, data, topic=None):
        """Sends a message to the provided Pub/Sub topic with the provided data payload.

        :param dict data: content of the message to be sent. Should contain 'stage' and 'mission_id'. Can contain any
                          additional JSON serializable information.
        :param string topic: Google Pub/Sub topic name, e.g. 'topic-for-stage'. This can either be provided here or be
                             set as a parameter for the stage as 'topic' or 'psq'.
        """

        publisher_client = pubsub_v1.PublisherClient()
        if self.project is None:
            raise ValueError(
                "Project is not set. Use GCPHouston.project = '[PROJECT]' "
                "or set 'GCP_PROJECT' environment variable"
            )

        if 'plan' not in data:
            data['plan'] = self.plan['name']

        # try to find the topic name in the stage parameters
        if topic is None:
            if 'stage' in data:
                stage_params = self.get_params(data['stage'])
                if stage_params:
                    if stage_params['topic']:
                        topic = stage_params['topic']
                    elif stage_params['psq']:
                        topic = stage_params['psq']

            if topic is None:
                raise ValueError("Pub/Sub could not be determined. It can either be provided as an argument to "
                                 "pubsub_trigger, or be a stage parameter with name 'topic' or 'psq'")

        full_topic = "projects/{project}/topics/{topic}".format(
            project=self.project, topic=topic
        )

        future = publisher_client.publish(topic=full_topic, data=json.dumps(data).encode("utf-8"))
        future.result()

    def call_stage_via_pubsub(self, response, mission_id):
        """Send stage details to Google Cloud Platform PubSub. Sends stage, mission_id, plan name as json in message
           body, parameters as attributes

           Message parameter must contain "psq" (PubSub Queue) key, this informs the function which topic is relevant
           to the task

           Blocks until PubSub message has been sent

        :param dict response: response from Houston.end_stage
        :param string mission_id: unique identifier of mission currently being completed
        """

        publisher_client = pubsub_v1.PublisherClient()
        if self.project is None:
            raise ValueError(
                "Project is not set. Use GCPHouston.project = '[PROJECT]' "
                "or set 'GCP_PROJECT' environment variable"
            )

        # for all available tasks - trigger qs
        for next_task in response["next"]:
            if next_task not in response["params"]:
                print(
                    "task: {next_task} does not have parameters, skipping".format(
                        next_task=next_task
                    )
                )
                continue
            if "psq" not in response["params"][next_task].keys() and "topic" not in response["params"][next_task].keys():
                print(
                    "task: {next_task} does not have psq topic set, skipping".format(
                        next_task=next_task
                    )
                )
                continue

            task_parameters = response["params"][next_task]
            if "psq" in task_parameters:
                target_psq = task_parameters.pop("psq")
            else:
                target_psq = task_parameters.pop("topic")

            data = json.dumps(
                {"stage": next_task, "mission_id": mission_id, "plan": self.plan}
            ).encode("utf-8")

            # make topic string
            topic = "projects/{project}/topics/{topic}".format(
                project=self.project, topic=target_psq
            )

            # json encode task param values
            # useful for decoding in PubSub subscriber
            for key, value in task_parameters.items():
                task_parameters[key] = json.dumps(value)

            if not task_parameters:
                future = publisher_client.publish(topic=topic, data=data)
                future.result()
            else:
                future = publisher_client.publish(
                    topic=topic, data=data, **task_parameters
                )
                future.result()

    @staticmethod
    def extract_stage_information(data):
        """Static method to extract stage information from sent PubSub message"""
        return json.loads(base64.b64decode(data))
