
import os
from io import BytesIO
from google.cloud import storage


def download_file_as_text(gs_uri):
    client = storage.Client(project=os.getenv('GCP_PROJECT', os.getenv('PROJECT_ID')))

    try:
        gs_bucket, gs_path = gs_uri.replace("gs://", "").split("/", 1)
    except ValueError:
        raise ValueError(f"Couldn't download plan; '{gs_uri}' is not a valid Google Storage URI. "
                         f"URI should look like 'gs://my-bucket/path/to/my-plan.yaml")

    bucket = client.get_bucket(gs_bucket)
    blob = bucket.get_blob(gs_path)

    if blob is None:
        raise ValueError(f"No object exists at GCS location: {gs_uri} - cannot load plan.")

    file_buffer = BytesIO()
    blob.download_to_file(file_buffer)
    file_buffer.seek(0)

    return file_buffer.read().decode("utf-8")
