import json
import os
import logging
from collections import defaultdict
from typing import *
import urllib.parse
from retry import retry
from houston.exceptions import HoustonClientError, HoustonException, HoustonServerBusy, \
                               HoustonServerError, HoustonPlanNotFound
from houston.interface import InterfaceRequest


HOUSTON_BASE_URL = os.getenv("HOUSTON_BASE_URL", "https://callhouston.io/api/v1")

retry_wrapper = retry((HoustonServerError, HoustonServerBusy, OSError), tries=3, delay=1, backoff=100)
log = logging.getLogger(os.getenv('HOUSTON_LOG_NAME', "houston"))


class Houston:

    def __init__(self, plan: Union[dict, str], api_key: str = None):
        """
        :param plan: Can be either:
                        - string: name of an existing plan to be loaded
                        - dict: plan definition, see Plan docs for example
                        If a plan definition is provided, ensure it is loaded with load_plan to make it
                        available to other instances
        :param api_key: Api key provided from https://callhouston.io, see account information. Can also be set as
                        environment variable HOUSTON_KEY
        """
        if api_key is None:
            api_key = self._find_api_key()

        if api_key is None:
            raise ValueError("No API key was found. Provide 'api_key' parameter or set the 'HOUSTON_KEY' environment "
                             "variable.")

        self.key = api_key
        self.interface_request = InterfaceRequest(key=api_key)
        self.base_url = HOUSTON_BASE_URL

        # TODO: load mission instead of plan
        if isinstance(plan, str):
            try:
                self.plan = self.get_plan(plan)  # look for existing saved plan
            except HoustonClientError:
                self.plan = self.import_plan(plan)  # look for file containing plan
        else:
            self.plan = plan

        print(self.plan)

        if "name" not in self.plan:
            raise HoustonClientError(
                "Sorry, this plan is not formatted correctly - must contain a name"
            )

    @staticmethod
    def _find_api_key() -> str:
        """Attempt to load the Houston API key from the environment.
        """
        return os.getenv("HOUSTON_KEY", None)

    @staticmethod
    def load_plan(path) -> str:
        """Load plan from a file.
        """
        try:
            with open(path) as f:
                plan = f.read()
            return plan

        except FileNotFoundError:
            raise HoustonPlanNotFound(f"No existing plans or plan files were found for '{path}'.")

    @classmethod
    def import_plan(cls, path: str) -> dict:

        plan = cls.load_plan(path)

        if ".yaml" in path or ".yml" in path:
            import yaml
            try:
                plan = yaml.load(plan, Loader=yaml.SafeLoader)
            except yaml.YAMLError as e:
                log.error(f"Plan has .yaml file extension but is not valid YAML")
                raise e

        else:  # assume plan is json
            try:
                plan = json.loads(plan)
            except json.JSONDecodeError as e:
                log.error("Plan is not valid JSON")
                raise e

        # convert any stage parameters to json strings if they are objects
        if not isinstance(plan, str):
            if 'stages' in plan:
                for i, stage in enumerate(plan['stages']):
                    if 'params' in stage:
                        for param in stage['params']:
                            if isinstance(stage['params'][param], dict) or isinstance(stage['params'][param], list):
                                plan['stages'][i]['params'][param] = json.dumps(stage['params'][param])

        return plan

    @retry_wrapper
    def save_plan(self):
        """Sends selected plan to Houston server"""
        self.interface_request.request(
            method="post", uri=self.base_url + "/plans", data=json.dumps(self.plan)
        )

    @retry_wrapper
    def delete_plan(self, safe=False):
        """Removes selected plan from Houston server

        :param boolean safe: Ignore exception raised by invalid request i.e. plan doesn't exist, True = ignore
        """
        plan_name = self.plan["name"]
        self.interface_request.request(
            "delete", uri=self.base_url + "/plans/" + plan_name, safe=safe
        )

    @retry_wrapper
    def get_plan(self, plan_name=None):
        """Get saved plan detail from Houston server

        :param string plan_name: [Optional] name of plan to retrieve details of
        :return dict: plan detail
        """
        if plan_name is None:
            plan_name = self.plan["name"]

        status_code, response = self.interface_request.request(
            "get", uri=self.base_url + "/plans/" + plan_name
        )
        return response

    @retry_wrapper
    def create_mission(self):
        """Creates a new instance of a mission
        :return string: new mission instance id or False if error
        """
        status_code, response = self.interface_request.request(
            "post",
            uri=self.base_url + "/missions",
            data=json.dumps({"plan": self.plan["name"]}),
        )
        if "id" not in response:
            raise HoustonException(
                "Create mission operation did not return a mission_id, please retry"
            )

        return response["id"]

    @retry_wrapper
    def start_stage(self, stage_name, mission_id, retry=3, ignore_dependencies=False):
        """Starts Houston stage, returns current stage parameters if available

        :param string stage_name: name of stage to start
        :param string mission_id: unique identifier of mission currently being completed
        :param int retry: number of retries in the case of failures, (429 responses are common with complex graphs)
        default is 3
        :param bool ignore_dependencies: if set to True, the stage will be allowed to start regardless of the state of
                                         all upstream stages dependencies
        :returns dict: Houston response {"next": list(string), "complete": bool, "params": dict()}
        """

        payload = {"state": "started", "ignoreDependencies": ignore_dependencies}
        status_code, json_response = self.interface_request.request(
            "post",
            uri=self.base_url + "/missions/" + mission_id + "/stages/" + stage_name,
            data=json.dumps(payload),
            retry=retry,
        )

        return json_response

    @retry_wrapper
    def end_stage(self, stage_name, mission_id, retry=3, ignore_dependencies=False):
        """Ends a Houston stage, returns downstream stages and available parameters

        :param string stage_name: name of stage which has finished
        :param string mission_id: unique identifier of mission currently being completed
        :param int retry: number of retry attempts
        :param bool ignore_dependencies: if set to True, all downstream stages dependant on this stage will be ignored,
                                         effectively ending the mission early
        :returns dict: Houston response {"next": list(string), "complete": bool, "params": dict(stage_name: dict())}
                       params contains multiple stage's parameters stored by stage name as keys
                       (e.g. {"next": "stage-two", "complete": False, "params": {"stage-two": dict()}})
        """

        payload = {"state": "finished", "ignoreDependencies": ignore_dependencies}
        status_code, json_response = self.interface_request.request(
            "post",
            uri=self.base_url + "/missions/" + mission_id + "/stages/" + stage_name,
            data=json.dumps(payload),
            retry=retry,
        )

        return json_response

    @retry_wrapper
    def fail_stage(self, stage_name, mission_id, retry=3):
        """Marks a Houston stage as failed, which allows it to be started again, returns downstream stages and available
        parameters

        :param string stage_name: name of stage which has failed
        :param string mission_id: unique identifier of mission currently being completed
        :param int retry: number of retry attempts
        :returns dict: Houston response {"next": list(string), "complete": bool, "params": dict(stage_name: dict())}
                       params contains multiple stage's parameters stored by stage name as keys
                       (e.g. {"next": "stage-two", "complete": False, "params": {"stage-two": dict()}})
        """

        payload = {"state": "failed"}
        status_code, json_response = self.interface_request.request(
            "post",
            uri=self.base_url + "/missions/" + mission_id + "/stages/" + stage_name,
            data=json.dumps(payload),
            retry=retry,
        )

        return json_response

    @retry_wrapper
    def ignore_stage(self, stage_name, mission_id, retry=3):
        """Marks a Houston stage as ignored. See https://callhouston.io/docs#ignored for more information

        :param string stage_name: name of stage which should be ignored
        :param string mission_id: unique identifier of mission currently being completed
        :param int retry: number of retry attempts
        :returns dict: Houston response {"next": list(string), "complete": bool, "params": dict()}
        """

        payload = {"state": "ignored"}
        status_code, json_response = self.interface_request.request(
            "post",
            uri=self.base_url + "/missions/" + mission_id + "/stages/" + stage_name,
            data=json.dumps(payload),
            retry=retry,
        )

        return json_response

    @retry_wrapper
    def skip_stage(self, stage_name, mission_id, retry=3):
        """Marks a Houston stage as skipped, meaning the rest of the mission will continue as if that stage doesn't
        exist.

        :param string stage_name: name of stage which should be ignored
        :param string mission_id: unique identifier of mission currently being completed
        :param int retry: number of retry attempts
        :returns dict: Houston response {"next": list(string), "complete": bool, "params": dict()}
        """

        payload = {"state": "skipped"}
        status_code, json_response = self.interface_request.request(
            "post",
            uri=self.base_url + "/missions/" + mission_id + "/stages/" + stage_name,
            data=json.dumps(payload),
            retry=retry,
        )

        return json_response

    def get_stage(self, stage_name: str) -> Optional[dict]:
        """Returns the full definition of a stage within the plan. Returns `None` if the stage
        doesn't exist."""
        filtered_stages = [s for s in self.plan['stages'] if s['name'] == stage_name]

        if len(filtered_stages) < 1:
            return None
        elif len(filtered_stages) > 1:
            raise ValueError("Can't return params because more than one stage in the plan has the name '{}'. "
                             "Plan is not valid.".format(stage_name))

        return filtered_stages[0]

    def get_params(self, stage_name: str) -> Optional[dict]:
        """Returns the parameters for the provided stage name as defined in the plan. Returns `None` if the stage
        doesn't exist. Note: The plan used to retrieve parameters is the latest version and is not linked to any
        particular mission ID, it is therefore possible for parameter values returned from this method to differ from
        those belonging to the current mission.

        :param string stage_name: name of a stage in the plan
        :return collections.defaultdict: stage parameters as key value pairs
        """

        this_stage = self.get_stage(stage_name)
        if this_stage is None:
            return None

        if 'params' in this_stage:
            params = this_stage['params']

        else:
            params = dict()

        return defaultdict(lambda: None, params)

    def get_service_from_stage_name(self, stage_name: str) -> Optional[dict]:
        if 'services' not in self.plan:
            return None

        this_stage = self.get_stage(stage_name)
        if this_stage is None:
            return None

        if 'service' in this_stage:
            filtered_services = [s for s in self.plan['services'] if s['name'] == this_stage['service']]
            if len(filtered_services) < 1:
                return None
            return filtered_services[0]
        else:
            return None

    @property
    def independent_stages(self) -> List[dict]:
        """Find stages in a plan that have no dependencies and should therefore be triggered to start a mission.
        :return: List of all independent stages
        """
        has_dependencies = []
        for stage in self.plan['stages']:
            if 'upstream' in stage and stage['upstream'] is not None and len(stage['upstream']) > 0:
                has_dependencies.append(stage['name'])
            if 'downstream' in stage and stage['downstream'] is not None:
                if isinstance(stage['downstream'], str):
                    has_dependencies.append(stage['downstream'])
                else:
                    has_dependencies += stage['downstream']

        return [s for s in self.plan['stages'] if s['name'] not in has_dependencies]

    def _validate_message_data(self, data) -> (dict, dict):

        stage = data.get("stage")
        if stage is None:
            raise ValueError("Triggering message data does not contain 'stage'.")
        if not isinstance(stage, str):
            raise ValueError(f"Triggering message has an invalid value for 'stage'. Expected string, got '{stage}'")

        params = self.get_params(stage)
        if params is None:
            raise ValueError(f"Stage '{stage}' has no params. Cannot trigger a stage without the relevant params.")

        if 'plan' not in data:
            data['plan'] = self.plan['name']

        return data, params

    @retry_wrapper
    def http_trigger(self, data: dict):
        """Trigger a stage of the plan via HTTP GET request. The contents of `data` will be passed as URL query
        parameters. The service specified for the stage is expected to have a trigger with method 'http' and a value for
        'url'. The request will be made without waiting for the result to ensure that the process that makes the request
        can end without the next stage needing to finish.
        :param dict data: content of the message to be sent. Should contain 'stage' and 'mission_id'. Can contain any
                          additional JSON serializable information.
        """
        stage = data.get('stage')
        if stage is None:
            raise ValueError("Cannot trigger stage: A stage name was not provided.")

        service = self.get_service_from_stage_name(stage)

        if service is None:
            raise ValueError(f"Cannot trigger stage '{stage}': no service is defined for this stage. "
                             f"See https://callhouston.io/docs#services.")

        url = service.get('trigger').get('url')

        query = urllib.parse.urlencode(data, doseq=False)  # convert content to url query

        self.interface_request.request(
            "GET",
            uri=f"{url}?{query}",
            fire_and_forget=True,
        )

    def trigger(self, data: dict):
        """Trigger a stage using the first available method based on the stage params.
        :param data: event data to be sent to each stage's service to trigger the stage. Should contain the name of the
                     stage and mission ID.
        """
        data, params = self._validate_message_data(data)

        # determine how to trigger stage
        service = self.get_service_from_stage_name(data['stage'])

        if service is not None:
            trigger_method = service['trigger']['method'].lower()
        elif 'topic' in params and 'topic_key' in params:
            trigger_method = 'azure/event-grid'
        elif any([p in params for p in ('psq', 'topic')]):
            trigger_method = 'google/pubsub'
        else:
            raise ValueError("Couldn't find a way to trigger the stage. Add the required information to the "
                             "stage definition. See docs: callhouston.io/docs#services")

        if trigger_method == 'google/pubsub' or trigger_method == 'pubsub':
            try:
                from houston.gcp import pubsub_trigger
                pubsub_trigger(self, data)
            except ImportError:
                raise ImportError(f"Cannot use Pub/Sub to trigger stage because GCP plugin is not installed. "
                                  f"Use: `pip install houston-client[gcp]`")
        elif trigger_method == 'azure/event-grid' or trigger_method == 'event-grid' or trigger_method == 'eventgrid':
            try:
                from houston.plugin.azure import event_grid_trigger
                event_grid_trigger(self, data)
            except ImportError:
                raise ImportError(f"Cannot use Event Grid trigger because Azure plugin is not installed."
                                  f"Use: `pip install houston-client[azure]`")

        elif trigger_method == "http":
            self.http_trigger(data)  # TODO: get endpoint from services as well

        else:
            raise ValueError(f"Trigger method '{trigger_method}' is not recognised. "
                             f"Use one of: http, google/pubsub, azure/event-grid.")

    def trigger_all(self, stages: List[str], mission_id: str,
                    ignore_dependencies: bool = False, ignore_dependants: bool = False, **kwargs):
        """Trigger multiple stages from a list of stage names. Triggering event data will be automatically generated.
        Any other keyword arguments provided will be sent to all stages.
        :param stages: list of stage names
        :param mission_id: unique identifier of mission currently being completed
        :param ignore_dependencies: If true, all stages will be triggered with instructions to ignore upstream dependencies
        :param ignore_dependants: If true, all stages will be triggered with instructions to upstream downstream stages
        """
        for stage in stages:
            self.trigger(dict(stage=stage, mission_id=mission_id, plan=self.plan['name'],
                              ignore_dependencies=ignore_dependencies, ignore_dependants=ignore_dependants, **kwargs))
