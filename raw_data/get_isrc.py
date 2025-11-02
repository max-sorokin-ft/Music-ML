from google.cloud import storage
import requests
import json
import logging
import time
from tqdm import tqdm
import argparse

from storage.gcs_utils import (
    get_artists_from_gcs,
    get_albums_from_gcs,
    get_artist_songs_from_gcs,
)
from auth import get_spotify_access_token

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


BUCKET_NAME = "music-ml-data"

def fetch_isrc_spotify(song_ids, max_retries=3, sleep_time=1):
    """Fetches the ISRC from the Spotify API, for a list of song ids"""
    for attempt in range(max_retries):
        try:
            url = "https://api.spotify.com/v1/tracks"
            headers = {"Authorization": f"Bearer {get_spotify_access_token()}"}
            params = {"ids": song_ids}
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                logger.warning(f"Rate limited by Spotify. Come back in {retry_after} seconds.")
                break

            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching ISRC from Spotify API: {e}")
            time.sleep(sleep_time)
    raise Exception(f"Error fetching ISRC from Spotify API: {e}")

def write_isrc_gcs(artists, bucket_name, base_blob_name):
    """Writes the ISRC to the GCS bucket"""
    try:
       for artist in artists:
           songs = get_artist_songs_from_gcs(artist, bucket_name)
           for song in songs:
               isrc = song.get("isrc")
               if isrc:
                   artist["isrc"] = isrc
               else:
                   artist["isrc"] = None
    except Exception as e:
        logger.error(f"Error writing ISRC to GCS bucket {bucket_name} with blob name {base_blob_name}: {e}")



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
        BUCKET_NAME,
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
    )