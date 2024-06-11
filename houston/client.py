import json
import os
import logging
import yaml

from retry import retry
from typing import Union, Dict, List, Optional

from .exceptions import HoustonClientError, HoustonException, HoustonServerBusy, HoustonServerError, HoustonPlanNotFound
from .interface import InterfaceRequest
from .plan import PlanTemplate
from .mission import Mission

HOUSTON_BASE_URL = os.getenv("HOUSTON_BASE_URL", "https://callhouston.io/api/v1")

retry_wrapper = retry((HoustonServerBusy, OSError), tries=3, delay=1, backoff=100)
log = logging.getLogger(os.getenv('HOUSTON_LOG_NAME', "houston"))


class Houston:

    def __init__(self, plan: Union[dict, str], api_key: str = None, base_url: str = None, auth: Optional[dict] = None):
        """
        :param plan: Can be either:
                        - string: name of an existing plan to be loaded
                        - string: local file path to a YAML or JSON file containing a plan definition
                        - dict: plan definition, see Plan docs for example
                     If a plan definition is provided, ensure it is saved with the `save_plan` method to make it
                     available to other instances.
        :param api_key: The API key corresponding to the account you wish to use.
                        Can also be set as environment variable HOUSTON_KEY.
        :param base_url: URL of the Houston server to be used. Can also be set as environment variable HOUSTON_BASE_URL.
                         If none is set then "https://callhouston.io/api/v1" will be used. The base URL can also be
                         provided within the `api_key` (or the HOUSTON_KEY environment variable) with the format:
                         '{base URL}/key/{key ID}', e.g. 'https://houston.example.com/api/v1/key/abc123'.
        :param auth: (optional) Map of service name to authentication parameters. See
                     https://github.com/datasparq-ai/houston/blob/main/docs/service_trigger_methods.md for
                     details on how to provide authentication for each type of authenticated trigger.
        """
        if api_key is None:
            api_key = self._find_api_key()

        if api_key is None:
            raise ValueError("No API key was found. Provide 'api_key' parameter or set the 'HOUSTON_KEY' environment "
                             "variable.")

        if str.startswith(api_key, "http://") or str.startswith(api_key, "https://"):
            split_key = api_key.split("/key/")
            if len(split_key) != 2:
                raise ValueError("Key has an invalid format. Expected format: '{base URL}/key/{key ID}'.")

            base_url = split_key[0]
            api_key = split_key[1]

        self.key = api_key
        self.interface_request = InterfaceRequest(key=api_key)

        if base_url is None:
            base_url = HOUSTON_BASE_URL
        self.base_url = base_url

        if auth is None:
            self.auth = {}
        else:
            self.auth = auth

        if isinstance(plan, str):
            try:
                self.plan = self.get_plan(plan)  # look for existing saved plan
            except HoustonClientError:
                self.plan = self.import_plan(plan)  # look for file containing plan
        else:
            self.plan = plan

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

        # substitute environment variable names
        plan = PlanTemplate(plan).safe_substitute(os.environ)

        if ".yaml" in path or ".yml" in path:
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
    def create_mission(self) -> str:
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
    def list_missions(self) -> List[str]:
        """Get all active (non archived) missions for the selected plan.
        :return: List of mission IDs
        """

        status_code, response = self.interface_request.request(
            "get", uri=self.base_url + f"/plans/{self.plan['name']}/missions"
        )

        return response

    @retry_wrapper
    def list_completed_missions(self) -> List[str]:
        """Get all completed (but not yet archived) missions associated with this key. These missions will also be in
        the list returned by `Houston.list_missions()`. Completed missions are eligible to be archived automatically
        by the API.
        :return: List of mission IDs
        """

        status_code, response = self.interface_request.request(
            "get", uri=self.base_url + f"/completed"
        )

        return response

    def list_missions_in_progress(self) -> List[str]:
        """Get all missions for the selected plan that are not completed. A mission is completed when all stages have
        are finished, excluded, or skipped. This method exists as an alternative to querying every mission individually.
        :return: List of mission IDs
        """
        return list(set(self.list_missions()) - set(self.list_completed_missions()))

    @retry_wrapper
    def get_mission(self, mission_id: str) -> Mission:
        """Get saved mission detail from Houston server

        :param mission_id: ID of mission to retrieve details of
        :return: plan detail
        """

        status_code, response = self.interface_request.request(
            "get", uri=self.base_url + "/missions/" + mission_id
        )

        return Mission(data=response)

    @retry_wrapper
    def delete_mission(self, mission_id, safe=True):
        """Deletes a mission given a mission id

        :param mission_id: unique identifier of mission requiring deletion
        :param boolean safe: Ignore exception raised by invalid request i.e. plan doesn't exist, True = ignore
        :return dict:
        """

        # Delete selected mission
        self.interface_request.request(
            "delete", uri=self.base_url + "/missions/" + mission_id, safe=safe
        )

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
        :param bool ignore_dependencies: if set to True, all downstream stages dependent on this stage will be ignored,
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

    def get_params(self, stage_name: str, mission_id: Optional[str] = None) -> Optional[dict]:
        """Returns the parameters for the provided stage name as defined in the plan, or the current mission if
        mission_id is provided. Returns `None` if the stage doesn't exist. Mission parameters will also be included in
        the return value if the mission_id is provided.
        Note: Any mission parameter that shares a name with a stage parameter will be overwritten by it.
        Note: params are returned in their raw form, i.e. not JSON parsed.

        :param stage_name: name of a stage in the plan
        :param mission_id: Mission from which to read the stage parameters. If not provided, the plan will be used.
        :return dict: stage parameters as key value pairs
        """
        if mission_id is not None:
            mission = self.get_mission(mission_id)
            mission_params = mission.params
            this_stage = mission.get_stage(stage_name)
            if this_stage is None:
                return None

            mission_params.update(this_stage.params)
            return mission_params

        else:
            # get stage params from plan
            this_stage = self.get_stage(stage_name)
            if this_stage is None:
                return None

            params = this_stage.get('params', dict())
            if params is None:
                params = dict()

            return params

    def get_service_from_stage_name(self, stage_name: str) -> Optional[dict]:
        if self.plan.get('services') is None:
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

        params = self.get_params(stage, mission_id=data.get("mission_id"))
        if params is None:
            params = dict()

        if 'plan' not in data:
            data['plan'] = self.plan['name']

        return data, params

    @retry_wrapper
    def http_trigger(self, data: dict):
        """Trigger a stage of the plan via HTTP POST request. The contents of `data` will be passed as the request body.
        The service specified for the stage is expected to have a trigger with method 'http' and a value for
        'url'. The request will be made without waiting for the result to ensure that the process that makes the request
        can end without the next stage needing to finish.

        For more information see: https://github.com/datasparq-ai/houston/blob/main/docs/service_trigger_methods.md

        :param dict data: content of the message to be sent. Should contain 'stage' and 'mission_id'. Can contain any
                          additional JSON serializable information.
        """
        stage = data.get('stage')
        if stage is None:
            raise ValueError("Cannot trigger stage: A stage name was not provided.")

        service = self.get_service_from_stage_name(stage)

        if service is None:
            raise ValueError(f"Cannot trigger stage '{stage}': no service is defined for this stage. "
                             f"See: https://github.com/datasparq-ai/houston/blob/main/docs/services.md")

        if service.get('trigger') is None:
            raise ValueError(f"Cannot trigger stage '{stage}': no trigger is defined for this stage's service "
                             f"'{service.get('name')}'. "
                             f"See: https://github.com/datasparq-ai/houston/blob/main/docs/service_trigger_methods.md")

        headers = {}

        service_auth_requirement = service.get('trigger').get('auth')

        if service_auth_requirement is not None:

            no_auth_error_msg = (f"Cannot trigger stage '{stage}': this stage's service "
                                 f"'{service.get('name')}' requires authentication, but none was provided when the "
                                 f"client was initialised. Provide an object like "
                                 f"'{{\"{service.get('name', 'my_service')}\": {{\"token\": \"foobar123\"}}}}' to "
                                 f"the `auth` parameter of any service that needs to be able to trigger it. "
                                 f"See: https://github.com/datasparq-ai/houston/blob/main/docs/service_trigger_methods.md")

            if self.auth is None:
                raise ValueError(no_auth_error_msg)

            auth = self.auth.get(service.get('name'))

            if auth is None:
                raise ValueError(no_auth_error_msg)

            if service_auth_requirement.lower() == "bearer":

                token = auth.get('token')
                if token is None or token == "":
                    raise ValueError(f"Cannot trigger stage '{stage}': this stage's service '{service.get('name')}' "
                                     f"requires Bearer authentication and no token was provided. Provide an object "
                                     f"like '{{\"{service.get('name', 'my_service')}\": {{\"token\": \"foobar12\"}}}}' "
                                     f"to the `auth` parameter. "
                                     f"See: https://github.com/datasparq-ai/houston/blob/main/docs/services.md")

                headers = {"Authorization": "Bearer " + token}

            else:
                raise ValueError(f"Cannot trigger stage '{stage}': this stage's service '{service.get('name')}' "
                                 f"requires {service_auth_requirement} authentication, but this is not one of the "
                                 f"supported authentication types. Use one of: 'Bearer'.")

        url = service.get('trigger').get('url')

        self.interface_request.request(
            "POST",
            uri=f"{url}",
            data=json.dumps(data),
            headers=headers,
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
            raise ValueError("Couldn't find a way to trigger the stage. Add the required information to the stage "
                             "definition. See docs: https://github.com/datasparq-ai/houston/blob/main/docs/services.md")

        if trigger_method == 'google/pubsub' or trigger_method == 'pubsub':
            try:
                from houston.gcp import pubsub_trigger
                pubsub_trigger(self, data)
            except ImportError:
                raise ImportError(f"Cannot use Pub/Sub to trigger stage because GCP plugin is not installed. "
                                  f"Use: `pip install \"houston-client[gcp]\"`")
        elif trigger_method == 'azure/event-grid' or trigger_method == 'event-grid' or trigger_method == 'eventgrid':
            try:
                from houston.plugin.azure import event_grid_trigger
                event_grid_trigger(self, data)
            except ImportError:
                raise ImportError(f"Cannot use Event Grid trigger because Azure plugin is not installed."
                                  f"Use: `pip install \"houston-client[azure]\"`")

        elif trigger_method == "http" or trigger_method == "https":
            self.http_trigger(data)

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
        if stages is None:
            return
        for stage in stages:
            self.trigger(dict(stage=stage, mission_id=mission_id, plan=self.plan['name'],
                              ignore_dependencies=ignore_dependencies, ignore_dependants=ignore_dependants, **kwargs))
