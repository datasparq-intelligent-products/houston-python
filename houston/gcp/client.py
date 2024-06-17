
import base64
import json
import os
from houston.client import Houston
from houston.exceptions import HoustonServerError, HoustonServerBusy, HoustonPlanNotFound
from houston.gcp.secret_manager import get_secret
from houston.gcp.cloud_storage import download_file_as_text
from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPIError, NotFound, InvalidArgument
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from retry import retry

try:
    _, PROJECT_ID = default()
except DefaultCredentialsError:
    PROJECT_ID = None

retry_wrapper = retry((HoustonServerError, HoustonServerBusy, OSError, GoogleAPIError), tries=3, delay=1, backoff=2)


@retry_wrapper
def pubsub_trigger(client: Houston, data: dict, topic: str = None):
    """Trigger used by any services that use the 'pubsub' trigger method.
    Sends a message to the provided Pub/Sub topic with the provided data payload.
    :param client: Instance of the Houston client class
    :param data: content of the message to be sent. Should contain 'stage' and 'mission_id'. Can contain any
                 additional JSON serializable information.
    :param topic: Google Pub/Sub topic name, e.g. 'topic-for-stage' (for a topic in the current GCP project), or full
                  topic ID, e.g. 'projects/my-gcp-project/topics/topic-for-service'. This can either be provided here,
                  or taken from the stage's service.
    """

    data, stage_params = client._validate_message_data(data)

    service = client.get_service_from_stage_name(data['stage'])
    project = None
    if service is not None and service.get('trigger') is not None and service.get('trigger').get('project'):
        project = service.get('trigger').get('project')

    # try to find the topic name in the stage parameters
    if topic is None:
        if service is not None and 'trigger' in service:
            topic = service['trigger'].get('topic')
        elif stage_params:
            if stage_params.get('topic'):
                topic = stage_params['topic']
            elif stage_params.get('psq'):
                topic = stage_params['psq']

        if topic is None:
            raise ValueError("Pub/Sub topic name could not be determined. It can either be provided as an argument to "
                             "pubsub_trigger, or be a stage parameter with name 'topic' or 'psq'")

    # check whether topic provided contains the project name, e.g. f"projects/{project}/topics/{topic}"
    topic_sections = topic.split("/")
    if len(topic_sections) == 4 and topic_sections[0] == "projects" and topic_sections[2] == "topics":
        project = topic_sections[1]
        topic = topic_sections[3]

    # if the project has not been provided along with the topic then assume we are using the current project
    if project is None:
        if hasattr(client, 'project'):
            project = client.project
        else:
            project = os.getenv("GCP_PROJECT", os.getenv("PROJECT_ID", None))

    if project is None or project.strip() == "":
        raise ValueError(
            "Can't publish Pub/Sub message because project is not set. Specify a 'project' in the service's trigger, "
            "or use GCPHouston.project = 'your-project-id', or set 'GCP_PROJECT' environment variable"
        )

    try:
        publisher_client = pubsub_v1.PublisherClient()
    except DefaultCredentialsError:
        raise Exception("Couldn't create a Cloud Pub/Sub publisher client because default credentials could "
                        "not be found. Use `gcloud auth application-default login` to create default credentials "
                        "on a local machine.")

    future = publisher_client.publish(topic=f"projects/{project}/topics/{topic}", data=json.dumps(data).encode("utf-8"))

    try:
        future.result()
    except NotFound:
        raise ValueError(f"Couldn't publish Pub/Sub message to topic '{topic}'. This could be because the "
                         f"GCP project '{project}' doesn't exist, or user does not have access to it.")
    except InvalidArgument:
        raise ValueError(f"Couldn't publish Pub/Sub message to topic '{topic}'. This could be because the "
                         f"GCP project ID '{project}' was formatted incorrectly.")


class GCPHouston(Houston):

    project = os.getenv("GCP_PROJECT", os.getenv("PROJECT_ID", PROJECT_ID))

    @staticmethod
    def load_plan(path):
        if len(path) > 4 and path[:5] == "gs://":  # download plan from cloud storage
            try:
                return download_file_as_text(path)
            except ValueError:
                raise HoustonPlanNotFound(f"Could not find Houston plan at Google Storage URI: {path}.")
        else:
            return Houston.load_plan(path)

    def _find_api_key(self) -> str:
        """Attempt to load the Houston API key from the environment or from Google Cloud Secret Manager.

        :return:
        """
        api_key = Houston._find_api_key()

        if api_key is None:
            # attempt to find the API key stored in Google Secret Manager
            try:
                return get_secret(name=os.getenv('HOUSTON_KEY_SECRET_NAME', 'houston-key'), project=self.project)
            except ValueError:
                raise ValueError("Houston API key could not be found in 'HOUSTON_KEY' environment variable and could "
                                 "not be loaded from Google Cloud Secret Manager. See the docs for alternative ways of "
                                 "supplying the API key: https://github.com/datasparq-ai/houston/blob/main/docs/google_cloud.md#providing-the-api-key")
        return api_key

    def pubsub_trigger(self, data: dict, topic: str = None):
        """Sends a message to the provided Pub/Sub topic with the provided data payload.
        Full documentation https://github.com/datasparq-ai/houston/blob/main/docs/services.md#trigger-methods

        :param data: content of the message to be sent. Should contain 'stage' and 'mission_id'. Can contain any
                     additional JSON serializable information.
        :param topic: Google Pub/Sub topic name, e.g. 'topic-for-stage'. This can either be provided here or be
                      set as a parameter for the stage as 'topic' or 'psq'.
        """
        return pubsub_trigger(self, data, topic)

    @staticmethod
    def extract_stage_information(data: str) -> dict:
        """Static method to extract stage information from sent PubSub message"""
        e = json.loads(base64.b64decode(data))

        e['ignore_dependencies'] = e.get('ignore_dependencies', False)
        e['ignore_dependants'] = e.get('ignore_dependants', False)

        # allow 'mission' as an alias for 'mission_id'
        if 'mission' in e and 'mission_id' not in e:
            e['mission_id'] = e['mission']

        return e
