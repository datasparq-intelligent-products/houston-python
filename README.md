# Houston

![PyPI](https://img.shields.io/pypi/v/houston-client)

Python Client for [Houston](https://callhouston.io), an open source tool for building low cost, scalable, platform-agnostic data pipelines. 
Teams can share data processes as microservices using a simple API.

Documentation: [github.com/datasparq-ai/houston/docs](https://github.com/datasparq-ai/houston/blob/main/docs)

## Installation

This client can be installed via pip:

```bash
pip install houston-client
```

If you're running Houston on Google Cloud Platform you will need the 'gcp' plugin:

```bash
pip install "houston-client[gcp]"
```

## Usage

To get started in under 15 minutes, see the quickstart repo: https://github.com/datasparq-intelligent-products/houston-quickstart-python 

This client has the following uses:
- Saving, updating, and deleting plans: see [save command](https://github.com/datasparq-ai/houston/blob/main/docs/commands.md#save), [delete command](https://github.com/datasparq-ai/houston/blob/main/docs/commands.md#delete)
- Starting missions: see [start command](https://github.com/datasparq-ai/houston/blob/main/docs/commands.md#start)
- Creating Houston microservices:
  - With Google Cloud Functions, see [Cloud Function Service Decorator](houston/gcp/cloud_function.py), [Google Cloud](https://github.com/datasparq-ai/houston/blob/main/docs/google_cloud.md), and [Python Quickstart GCP](https://github.com/datasparq-intelligent-products/houston-quickstart-python/tree/master/google-cloud)
  - With any service: see https://github.com/datasparq-ai/houston/blob/main/docs/services.md, and [service.py](houston/service.py)

Full documentation is available at [github.com/datasparq-ai/houston](https://github.com/datasparq-ai/houston/tree/main/docs)


## Requirements

- Python >= 3.7
