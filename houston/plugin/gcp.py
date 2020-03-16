"""Houston Utilities for Google Cloud Platform

PubSub utils:
Allows user to create subscriber/publish message according to the plan stage options, e.g.:

    res = h.end_stage("load-data", id)
    next_stage = res["next"]
    h.gcp.trigger(next_stage, id)

--> finds the topic it needs to use for the next_task from the stage parameters in the plan statement
--> sends a pub/sub message to the next task's topic

or:

    h.gcp.project = "my-project-1234"
    h.gcp.topic = "ps-topic-next-stage"

    res = h.end_stage("load-data", id)
    next_stage = res["next"]
    h.gcp.trigger_stage_via_pubsub(next_stage, id)

"""

import base64
import json
import os
from google.cloud import pubsub_v1
from houston.client import Houston


class GCPHouston(Houston):

    project = os.getenv("GCP_PROJECT", None)
    topic = None

    def call_stage_via_pubsub(self, response, mission_id):
        """Send stage details to Google Cloud Platform PubSub. Sends stage, mission_id, plan name as json in message
           body, parameters as attributes

           Message parameter must contain "psq" (PubSub Queue) key, this informs the function which topic is relevant
           to the task

           Blocks until PubSub message has been sent

        :param dict response: Response from Houston.end_stage
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
            if "psq" not in response["params"][next_task].keys():
                print(
                    "task: {next_task} does not have psq topic set, skipping".format(
                        next_task=next_task
                    )
                )
                continue

            task_parameters = response["params"][next_task]
            target_psq = task_parameters.pop("psq")

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
