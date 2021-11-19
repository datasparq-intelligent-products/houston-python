
from google.cloud import secretmanager
from google.api_core.exceptions import GoogleAPIError, PermissionDenied, NotFound
from retry import retry
import os

GCP_PROJECT = os.getenv('GCP_PROJECT', os.getenv('PROJECT_ID'))


@retry((OSError, AttributeError, GoogleAPIError), tries=3, delay=1, backoff=10)
def get_secret(name: str, version: str = "latest", project=GCP_PROJECT) -> str:

    if project is None:
        raise ValueError(f"Project is unknown. Please provide the GCP project ID with the 'GCP_PROJECT' or 'PROJECT_ID' "
                         f"environment variable.")

    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project=project, secret=name, secret_version=version)

    try:
        response = client.access_secret_version(name=name)
    except NotFound:
        raise ValueError(f"Secret '{name}' was not found.")
    except PermissionDenied:
        raise ValueError(f"Permission denied when trying to access secret '{name}'. "
                         f"Caller must have roles/secretmanager.secretAccessor to get secrets values.")

    return response.payload.data.decode('UTF-8')

