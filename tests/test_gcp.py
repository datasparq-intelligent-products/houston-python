import time
import unittest
import sys

if sys.version_info >= (3, 3):
    from unittest import mock
else:
    import mock
from houston.plugin.gcp import GCPHouston
from tests.test_houston import MockResponse


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
        "stages": [
            {"name": "start", "downstream": ["end"], "params": {"table": "test.sql"}},
            {"name": "end", "params": {"table": "test.sql", "psq": "tp-test-end"}},
        ],
    }

    def test_call_stage_via_pubsub(self):
        with mock.patch(
            "houston.plugin.gcp.pubsub_v1.PublisherClient"
        ) as pubsub_client:
            with mock.patch("houston.interface.requests.request") as http:
                http.return_value = MockResponse(
                    status_code=200,
                    json_data={
                        "success": True,
                        "complete": False,
                        "next": ["end"],
                        "params": {"end": {"table": "test.sql", "psq": "tp-test-end"}},
                    },
                )
                houston = GCPHouston(
                    api_key="test-key", plan=self.test_plan_description
                )
                response = houston.end_stage("start", "test-launch-id")
                pubsub_client.return_value = MockPubSubResponse
                houston.project = "test-gcp-project"
                houston.call_stage_via_pubsub(response, "test-launch-id")

    def test_pubsub_trigger(self):
        with mock.patch(
            "houston.plugin.gcp.pubsub_v1.PublisherClient"
        ) as pubsub_client:
            with mock.patch("houston.interface.requests.request") as http:
                http.return_value = MockResponse(
                    status_code=200,
                    json_data={
                        "success": True,
                        "complete": False,
                        "next": ["end"],
                        "params": {"end": {"table": "test.sql", "psq": "tp-test-end"}},
                    },
                )
                houston = GCPHouston(
                    api_key="test-key", plan=self.test_plan_description
                )
                response = houston.end_stage("start", "test-launch-id")
                pubsub_client.return_value = MockPubSubResponse
                houston.project = "test-gcp-project"

                for next_stage in response['next']:
                    topic = houston.get_params(next_stage)['psq']
                    houston.pubsub_trigger({'stage': next_stage, 'mission_id': "test-launch-id"}, topic)


if __name__ == "__main__":
    unittest.main()
