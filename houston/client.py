import json
import os

from houston.exceptions import HoustonClientError, HoustonException
from houston.interface import InterfaceRequest
from collections import defaultdict

try:
    basestring
except NameError:
    basestring = str


class Houston:
    def __init__(self, plan, api_key=None):
        """
        :param plan: Can be either:
                        - string: name of an existing plan to be loaded
                        - dict: plan definition, see Plan docs for example
                        If a plan definition is provided, ensure it is loaded with load_plan to make it
                        available to other instances
        :param string api_key: Api key provided from https://callhouston.io, see account information. Can also be set as
                        environment variable HOUSTON_KEY
        """
        if api_key is None:
            api_key = os.getenv("HOUSTON_KEY", None)
        if api_key is None:
            raise ValueError(
                "No API key was found. Provide 'api_key' parameter or set the "
                "HOUSTON_KEY environment variable."
            )
        self.key = api_key
        self.interface_request = InterfaceRequest(key=api_key)
        self.base_url = "https://callhouston.io/api/v1"

        if isinstance(plan, str):
            self.plan = self.get_plan(plan)
        else:
            self.plan = plan

        if "name" not in self.plan:
            raise HoustonClientError(
                "Sorry, this plan is not formatted correctly - must contain a name"
            )

    def save_plan(self):
        """Sends selected plan to Houston server"""
        self.interface_request.request(
            method="post", uri=self.base_url + "/plans", data=json.dumps(self.plan)
        )

    def delete_plan(self, safe=False):
        """Removes selected plan from Houston server

        :param boolean safe: Ignore exception raised by invalid request i.e. plan doesn't exist, True = ignore
        """
        plan_name = self.plan["name"]
        self.interface_request.request(
            "delete", uri=self.base_url + "/plans/" + plan_name, safe=safe
        )

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

    def get_params(self, stage_name):
        """Returns the parameters for the provided stage name as defined in the plan. Returns `None` if the stage
        doesn't exist. Note: The plan used to retrieve parameters is the master version and is not linked to any
        particular mission ID, it is therefore possible for parameter values returned from this method to differ from
        those belonging to the current mission.

        :param string stage_name: name of a stage in the plan
        :return collections.defaultdict: stage parameters as key value pairs
        """

        filtered_stages = [s for s in self.plan['stages'] if s['name'] == stage_name]

        if len(filtered_stages) < 1:
            return None
        elif len(filtered_stages) > 1:
            raise ValueError("Can't return params because more than one stage in the plan has the name '{}'. "
                             "Plan is not valid.".format(stage_name))

        this_stage = filtered_stages[0]

        if 'params' in this_stage:
            params = this_stage['params']

        else:
            params = dict()

        return defaultdict(lambda: None, params)
