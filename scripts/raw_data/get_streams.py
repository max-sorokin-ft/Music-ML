import requests
from bs4 import BeautifulSoup
from auth import get_spotify_access_token
from tqdm import tqdm
from datetime import datetime
import json
import time
import logging
from google.cloud import storage
import argparse

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--page_number",
        type=int,
        default=1,
        help="The number of the kworb's page",
    )
    parser.add_argument(
        "--batch_number",
        type=int,
        default=1,
        help="The batch number of the artists",
    )
    args = parser.parse_args()
    artists = get_artists_from_gcs(
        "music-ml-data",
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
    )
