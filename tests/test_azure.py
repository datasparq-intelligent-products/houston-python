import time
import unittest
import sys

if sys.version_info >= (3, 3):
    from unittest import mock
else:
    import mock
from houston.plugin.azure import AzureHouston
from tests.test_houston import MockResponse


class MockEventGridResponse:
    @staticmethod
    def publish_events(topic, events):
        return None


class TestEventGrid(unittest.TestCase):

    test_plan_description = {
        "name": "test",
        "stages": [
            {"name": "start", "downstream": ["end"], "params": {"table": "test.sql"}},
            {"name": "end", "params": {"table": "test.sql", "topic": "tp-test-end", "topic_key": "abc"}},  # note: it is not recommeneded to keep keys in stage params
        ],
    }

    def test_event_grid_trigger(self):
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

                for next_stage in response['next']:
                    params = houston.get_params(next_stage)
                    houston.event_grid_trigger({'stage': next_stage, 'mission_id': "test-launch-id"},
                                               topic=params['topic'], topic_key=params['topic_key'])


if __name__ == "__main__":
    unittest.main()
