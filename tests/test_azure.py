import time
import unittest
import sys

if sys.version_info >= (3, 3):
    from unittest import mock
else:
    import mock
from houston.plugin.azure import AzureHouston

from . import mock_mission_data
from .test_houston import MockResponse


class MockEventGridResponse:
    @staticmethod
    def publish_events(topic, events):
        return None


class TestEventGrid(unittest.TestCase):

    test_plan_description = {
        "name": "test",
        "services": [{
            "name": "azure-functon",
            "trigger": {"method": "eventgrid", "topic": "tp-test-end", "topic_key": "abc"}
        }],
        "stages": [
            {"name": "start", "service": "azure-functon", "downstream": ["end"], "params": {"table": "test.sql"}},
            {"name": "end", "service": "azure-functon", "params": {"table": "test.sql", "topic": "tp-test-end", "topic_key": "abc"}},  # note: it is not recommeneded to keep keys in stage params
        ],
    }

    def test_event_grid_trigger(self):
        # this tests that the Azure client can automatically determine that the stage's service uses event grid trigger
        # and finds the trigger topic

        with mock.patch(
            "houston.plugin.azure.EventGridClient"
        ) as event_grid_client:
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
                houston = AzureHouston(
                    api_key="test-key", plan=self.test_plan_description
                )
                response = houston.end_stage("start", "test-launch-id")
                event_grid_client.return_value = MockEventGridResponse

            with mock.patch("houston.interface.requests.request") as http:
                # mock response for GET /mission/test-launch-id
                http.return_value = MockResponse(
                    status_code=200,
                    json_data=mock_mission_data,
                )
                for next_stage in response['next']:
                    params = houston.get_params(next_stage, mission_id="test-launch-id")
                    houston.trigger({'stage': next_stage, 'mission_id': "test-launch-id"})


if __name__ == "__main__":
    unittest.main()
