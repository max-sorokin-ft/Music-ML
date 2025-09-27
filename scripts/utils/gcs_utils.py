from google.cloud import storage
import json
import logging

logger = logging.getLogger(__name__)


def get_artists_from_gcs(bucket_name, blob_name):
    """Gets the artists from the gcs bucket"""
    try:
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        artists = json.loads(blob.download_as_string())
        return artists
    except Exception as e:
        logger.error(
            f"Error getting artists from gcs bucket {bucket_name} with blob name {blob_name}: {e}"
        )
        raise Exception(
            f"Error getting artists from gcs bucket {bucket_name} with blob name {blob_name}: {e}"
        )
