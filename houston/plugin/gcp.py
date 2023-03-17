"""Houston Utilities for Google Cloud Platform

PubSub utils:
Allows user to create Google Cloud Pub/Sub message according to the plan stage options, e.g.:

    h.project = "my-project-1234"  # set the Google Cloud project name in the client

    res = h.end_stage("load-data", mission_id)

    for next_stage in res['next']:

        h.pubsub_trigger({'stage': next_stage, 'mission_id': mission_id}, topic=h.get_params(next_stage)['topic'])

--> sends a Pub/Sub message to the next tasks' topics. This assumes we have given each stage a 'topic' parameter.

Note: The topic name can either be provided as an argument or can be set as a parameter for the stage as 'topic' or
'psq', in which case it will be found automatically.

or:

    h.project = "my-project-1234"  # set the Google Cloud project name in the client

    response = h.end_stage("load-data", mission_id)

    h.call_stage_via_pubsub(response, mission_id)  # assumes each stage has a 'psq' parameter which gives the topic name

"""

from houston.gcp.client import GCPHouston
