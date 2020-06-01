import sys
import unittest

if sys.version_info >= (3, 3):
    from unittest import mock
else:
    import mock
from houston.client import Houston
from houston.exceptions import HoustonClientError, HoustonServerError


class MockResponse:
    """Requests response mock"""

    def __init__(self, status_code, json_data):
        self.status_code = status_code
        self.json_data = json_data
        self.headers = {"Content-Type": "application/json"}

    def json(self):
        return self.json_data


class TestPlan(unittest.TestCase):
    test_plan_description = {"name": "test", "stages": [{"name": "start"}]}

    def test_get_plan_success(self):
        with mock.patch("houston.interface.requests.request") as http:
            http.return_value = MockResponse(
                status_code=200, json_data=self.test_plan_description
            )
            houston = Houston(api_key="test", plan=self.test_plan_description)
            response = houston.get_plan()
            self.assertEqual(response, self.test_plan_description)

    def test_get_plan_client_error(self):
        with mock.patch("houston.interface.requests.request") as http:
            http.return_value = MockResponse(status_code=400, json_data={"error": ""})
            houston = Houston(api_key="test", plan=self.test_plan_description)
            self.assertRaises(HoustonClientError, houston.get_plan)

    def test_get_plan_server_error(self):
        with mock.patch("houston.interface.requests.request") as http:
            http.return_value = MockResponse(status_code=500, json_data={"error": ""})
            houston = Houston(api_key="test", plan=self.test_plan_description)
            self.assertRaises(HoustonServerError, houston.get_plan)

    def test_post_save_plan(self):
        with mock.patch("houston.interface.requests.request") as http:
            http.return_value = MockResponse(
                status_code=200, json_data=self.test_plan_description
            )
            houston = Houston(api_key="test", plan=self.test_plan_description)
            houston.save_plan()

    def test_delete_plan(self):
        with mock.patch("houston.interface.requests.request") as http:
            http.return_value = MockResponse(status_code=200, json_data=None)
            houston = Houston(api_key="test", plan=self.test_plan_description)
            houston.delete_plan()

    def test_delete_plan_safe(self):
        with mock.patch("houston.interface.requests.request") as http:
            http.return_value = MockResponse(status_code=400, json_data=None)
            houston = Houston(api_key="test", plan=self.test_plan_description)
            houston.delete_plan(safe=True)


class TestStage(unittest.TestCase):
    test_plan_description = {
        "name": "test",
        "stages": [
            {"name": "start", "downstream": ["end"], "params": {"table": "test.sql"}},
            {"name": "end", "params": {"table": "test.sql"}},
        ],
    }

    def test_start_stage(self):
        with mock.patch("houston.interface.requests.request") as http:
            http.return_value = MockResponse(
                status_code=200,
                json_data={
                    "next": [],
                    "complete": False,
                    "params": {"table": "test.sql"},
                },
            )
            houston = Houston(api_key="test", plan=self.test_plan_description)
            houston.start_stage("test", "launch-id")

    def test_get_params(self):
        houston = Houston(api_key="test", plan=self.test_plan_description)

        params = houston.get_params("start")
        self.assertEqual(params['table'], "test.sql")
        self.assertIs(params['notaparam'], None)

        not_params = houston.get_params("notastage")
        self.assertIs(not_params, None)


if __name__ == "__main__":
    unittest.main()
