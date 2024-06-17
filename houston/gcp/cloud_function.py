"""Utilities to integrate a Google Cloud Functions with Houston
"""

import os
import logging
from functools import wraps
from typing import Callable, Optional

from retry import retry
from google.api_core.exceptions import GoogleAPIError
from google.cloud import logging_v2 as cloud_logging
from google.cloud.logging_v2.resource import Resource
from google.auth.exceptions import DefaultCredentialsError

from houston.gcp.client import GCPHouston, PROJECT_ID
from houston.service import execute_service

FUNCTION_NAME = os.getenv('FUNCTION_NAME', os.getenv('FUNCTION_TARGET', "houston-cloud-function"))

log = logging.getLogger(os.getenv('HOUSTON_LOG_NAME', "cloudfunctions.googleapis.com%2Fcloud-functions"))
log.setLevel(os.getenv('LOG_LEVEL', 'INFO'))

try:
    # set up logging for Google Cloud Function
    log_handler = cloud_logging.Client().get_default_handler(
        resource=Resource(type="cloud_function", labels={
            "function_name": FUNCTION_NAME,
            "region": os.getenv('FUNCTION_REGION'),
            "project_id": os.getenv('GCP_PROJECT', PROJECT_ID)}))
    log.addHandler(log_handler)
except DefaultCredentialsError:
    pass


retry_wrapper = retry((OSError, AttributeError, GoogleAPIError), tries=3, backoff=3, delay=3, logger=log)


def service(name: str = "unnamed", auth=None, time_limit_seconds: int = os.getenv('FUNCTION_TIMEOUT_SEC', 300),
            wait_callback: Optional[Callable[..., bool]] = None, wait_interval_seconds: int = 10):
    """
    For full documentation, see:
    - https://github.com/datasparq-ai/houston/blob/main/docs/services.md
    - https://github.com/datasparq-ai/houston/blob/main/docs/google_cloud.md

    Wrapper to convert any Python function to a Houston service that can be executed by a Google Cloud Function
    using a Pub/Sub trigger. Example usage:

        # main.py

        from houston.gcp.cloud_function import service

        @service(name="My Service")
        def main(param_1, param_2):
            print("hello")

    The above function could be deployed with:

        gcloud functions deploy my-function-name --runtime python39 --trigger-topic my-function-topic \
          --source . --entry-point main --timeout 540 --set-env-vars GCP_PROJECT=my-project-id

    Note: this also requires a requirements.txt to be present, which must contain houston-client[gcp].

    The function can then execute stages, provided the Pub/Sub topic name is provided in the stage params in your plan.
    A message like the following would trigger it to run a stage:

        {"plan": "my-plan", "stage": "my-stage", "mission_id": "a0234gyil344enbp"}

    The stage parameters will be provided to the wrapped function as they are defined in the plan. For example, the
    stage config for the above example function could look like the following:

        - name: my-stage
          params:
            service: my-function
            param_1: "foo"
            param_2: 123

    There must also be a service definition in the plan. For the example function above, it could look like this:

        - name: my-function
          trigger:
            method: pubsub
            topic: my-function-topicn  # this tells the houston client how to trigger the service

    Messages can also contain Houston commands, e.g. a message like the following would save/update a plan.

        {"plan": "gs://my-bucket/my-plan.yaml", "command": "save"}

    The following would start a new mission:

        {"plan": "my-plan", "command": "start"}

    For more examples and all other commands see: https://github.com/datasparq-ai/houston/blob/main/docs/commands.md
    """

    log.info(f"Creating Houston service '{name}' within {FUNCTION_NAME}.")

    def outer(func):

        @wraps(func)
        def inner(event, context=None):

            # event may just be the message as a dict, as in the case of function testing events
            e = GCPHouston.extract_stage_information(event['data']) if 'data' in event else event

            log.info(f"Starting {name} Cloud Function with event data: {e}")

            res = execute_service(name=name,
                                  func=retry_wrapper(func),
                                  event=e,
                                  auth=auth,
                                  wait_callback=wait_callback,
                                  time_limit_seconds=time_limit_seconds,
                                  wait_interval_seconds=wait_interval_seconds,
                                  log=log,
                                  client_cls=GCPHouston)

            return res
        return inner
    return outer
