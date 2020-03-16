![Houston logo](https://storage.googleapis.com/gcs-callhouston-asset/houston-title.png)

Houston Python Client Library (https://callhouston.io), links components to a simple workflow api.

## Installation

This client can be easily installed via pip:

```commandline
pip install houston-client
```

## Requirements

- Python 3.7

## Proxy

Should you need to use a proxy for your Houston requests, please set them as an environment variable:

```bash
# windows
set https_proxy=10.0.0.1 

# linux
export https_proxy=10.0.0.1

```

## Usage

Please read the documentation before getting started: https://callhouston.io/docs

Now ready, you'll need to initialise the Houston object with both an api_key and a mission 
(either the name if already saved or dict of new plan):

```python
from houston.client import Houston

houston = Houston(api_key="H...", plan="test-plan")
```

### Plan

A plan is the description of your workflow, to set a new plan please load the json to a dict. 
This can now be easily saved:


```python
from houston.client import Houston

houston = Houston(api_key="H...", plan=dict())
houston.save_plan()
```

To return the plan in dictionary format, enter the plan name as a string:

```python
from houston.client import Houston

houston = Houston(api_key="H...", plan=dict() or str())
houston.get_plan()
```

To remove the plan from Houston:
Note: Extra parameter [safe] available to ignore any invalid responses e.g. 400 - plan does not exist
      True, ignores any errors | False (default), raises any invalid responses 
      
```python
from houston.client import Houston

houston = Houston(api_key="H...", plan=dict())
houston.delete_plan(safe=True)
```

### Mission

A mission is an instance of a plan, a workflow. To start a mission, first use the methods create_mission to get a 
mission UUID.

```python
from houston.client import Houston

houston = Houston(api_key="H...", plan=dict())
mission_id = houston.create_mission()
```

### Stage

Once a mission has been created, the client can be used to modify a state's status and progress through the workflow. 
Start / end stage return the JSON response from the api as a dict for downstream use. 

Response keys include: 

- next: list, of downstream stages available to be started
- complete: boolean, True if mission is complete
- params: dict, contains key / value parameters if stage starting or stage names as keys and params as values if ending

Example of stage starting & ending:

```python
from houston.client import Houston

houston = Houston(api_key="H...", plan="test-plan")
mission_id = houston.create_mission()
houston_start_response = houston.start_stage("test-stage", mission_id=mission_id)

# Note: only current task parameters returned as dict
stage_parameters = houston_start_response["params"]

# perform task, handle errors

houston_end_response = houston.end_stage("test-stage", mission_id=mission_id)

# Note: all available downstream task parameters returned as dict, keys are names of stages, values are dict of params 
next_task_parameters = houston_end_response["params"]["next-task"]

```

## Plugins

Plugins allow for Houston to easily integrate with external tools

### Google Cloud Pub/Sub

[Google Cloud Pub/Sub Documentation](https://cloud.google.com/pubsub/docs/overview)

Google Cloud Pub/Sub plugin publishes Houston responses to Topics, using Pub/Sub as a message bus between subscribing 
services.

To use this plugin, each stage must have a parameter named "psq" which defines the Pub/Sub Topic the stage service is 
listening to. 

Note: stage parameters must NOT include protected keys: "topic" & "data"

Before this plugin can be used, you must first create a Pub/Sub Topic and a Subscribing service which carries out your 
stage operations. When complete - the function call_stage_via_pubsub can be used to easily trigger downstream 
stages via Pub/Sub. For example:

```python
# import GCP Houston plugin
from houston.plugin.gcp import GCPHouston

houston = GCPHouston(api_key="H...", plan="test-plan")
mission_id = houston.create_mission()
houston.start_stage("test-stage", mission_id=mission_id)

# perform task, handle errors

response = houston.end_stage("test-stage", mission_id=mission_id)
houston.call_stage_via_pubsub(response, mission_id=mission_id)
```

Stage information and parameters are encoded via both the message body & attributes:

#### Message Body

The message body (base64 encoded) contains a JSON object of the key stage information:

```json
{"stage":  "name of stage",
 "mission_id":  "id of current mission",
 "plan":  "name of plan"}
```

For convenience, a method "extract_stage_information" is provided in the plugin to decode the message body: 

```python
from houston.plugin.gcp import GCPHouston

def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic. Calls Houston to start stage named in event payload, executes
    task function, calls Houston to finish stage.

    :param dict event: Event payload - expected to contain Houston 'stage' and 'mission_id'.
    :param google.cloud.functions.Context context: Metadata for the event.
    """
    houston = GCPHouston(api_key="H...", plan="test-plan")
    houston.extract_stage_information(event["data"])
```


### Message Attributes

The message attributes contain the key: value pairs of the parameters of the stage (JSON encoded). They can be loaded 
via the event attributes: 

```python
import json

def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic. Calls Houston to start stage named in event payload, executes
    task function, calls Houston to finish stage.

    :param dict event: Event payload - expected to contain Houston 'stage' and 'mission_id'.
    :param google.cloud.functions.Context context: Metadata for the event.
    """
    parameters = json.loads(event["attributes"])
```
