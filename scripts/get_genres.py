import requests
from auth import get_lastfm_api_key
import json
from google.cloud import storage
import logging
import argparse
from tqdm import tqdm
import re

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
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

# if __name__ == "__main__":
#     json_data = get_genres()
#     formatted_data = json.dumps(json_data, indent=3)
#     print(formatted_data)