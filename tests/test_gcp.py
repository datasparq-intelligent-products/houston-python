import time
import unittest
import sys
import base64
import json

if sys.version_info >= (3, 3):
    from unittest import mock
else:
    import mock
from houston.gcp.client import GCPHouston
from houston.gcp.cloud_function import service
from tests.test_houston import MockResponse

from . import mock_mission_data


class MockFuture:
    """Mock PubSub future object"""

    @staticmethod
    def result():
        time.sleep(0.1)
        return


class MockPubSubResponse:
    """Mock GCP PubSub response"""

    @staticmethod
    def publish(topic, data, **kwargs):
        return MockFuture


class TestCallStageViaPubSub(unittest.TestCase):

    test_plan_description = {
        "name": "test",
        "services": [
            {"name": "mock-service", "trigger": {"method": "pubsub", "topic": "projects/foo/topics/bar"}}
        ],
        "stages": [
            {"name": "start", "downstream": ["end"], "params": {"table": "test.sql"}},
            {"name": "end", "service": "mock-service", "params": {"table": "test.sql"}},
        ],
    }

    def test_call_stage_via_pubsub(self):
        with mock.patch(
            "houston.gcp.client.pubsub_v1.PublisherClient"
        ) as pubsub_client:
            with mock.patch("houston.interface.requests.request") as http:
                http.return_value = MockResponse(
                    status_code=200,
                    json_data={
                        "success": True,
                        "complete": False,
                        "next": ["end"],
                    },
                )
                houston = GCPHouston(
                    api_key="test-key", plan=self.test_plan_description
                )
                response = houston.end_stage("start", "test-launch-id")
                pubsub_client.return_value = MockPubSubResponse

            with mock.patch("houston.interface.requests.request") as http:
                # mock response for GET /mission/test-launch-id
                http.return_value = MockResponse(
                    status_code=200,
                    json_data=mock_mission_data,
                )
                houston.trigger_all(response['next'], "test-launch-id")

    def test_pubsub_trigger(self):
        with mock.patch(
            "houston.gcp.client.pubsub_v1.PublisherClient"
        ) as pubsub_client:
            with mock.patch("houston.interface.requests.request") as http:
                http.return_value = MockResponse(
                    status_code=200,
                    json_data={
                        "success": True,
                        "complete": False,
                        "next": ["end"],
                    },
                )
                houston = GCPHouston(
                    api_key="test-key", plan=self.test_plan_description
                )
                response = houston.end_stage("start", "test-launch-id")
                pubsub_client.return_value = MockPubSubResponse

            with mock.patch("houston.interface.requests.request") as http:
                # mock response for GET /mission/test-launch-id
                http.return_value = MockResponse(
                    status_code=200,
                    json_data=mock_mission_data,
                )
                for next_stage in response['next']:
                    houston.pubsub_trigger({'stage': next_stage, 'mission_id': "test-launch-id"})


class TestCloudFunctionService(unittest.TestCase):

    def test_create_cloud_function_service(self):
        def cf_func(param1: str, param2: int, param3: dict):
            # store
            assert param1 == "foo"
            assert param2 == 123
            assert param3["a"] == "foo"
            assert param3["b"] == 123

        cloud_function = service(name="my cloud function")(cf_func)

        params = dict(
            param1="foo",
            param2=123,
            param3=dict(a="foo", b=123)
        )

        cloud_function(event=dict(data=base64.b64encode(json.dumps(params).encode("utf-8"))), context=None)


if __name__ == "__main__":
    unittest.main()
