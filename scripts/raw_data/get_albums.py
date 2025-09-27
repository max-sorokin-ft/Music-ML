from google.cloud import storage
from auth import get_spotify_access_token
import requests
import json
import logging
import time
from tqdm import tqdm
import argparse

from scripts.utils.gcs_utils import get_artists_from_gcs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

"""
    This script is part of the data acquisition pipeline for the project and it is used to get the albums for a given artist from the spotify api.
    It loops through the artists from a given kworb page and gets the albums for each artist from the spotify api.
    The json data is uploaded to a gcs bucket.
"""

def get_albums_from_spotify(spotify_artist_id, token, max_retries=3, sleep_time=1):
    """Gets the albums from the spotify api for a given artist"""
    url = f"https://api.spotify.com/v1/artists/{spotify_artist_id}/albums"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"limit": 50, "include_groups": "album,single", "market": "US"}

    all_album_items = []
    page_url, page_params = url, params

    while True:
        success = False
        for attempt in range(max_retries):
            try:
                response = requests.get(page_url, headers=headers, params=page_params, timeout=10)

                response.raise_for_status()
                data = response.json()
                success = True
                break
            except Exception as e:
                backoff_time = sleep_time * (2 ** attempt)
                logger.warning(
                    f"Error getting artist's albums page from Spotify: {e}. Retrying in {backoff_time} seconds (attempt {attempt+1}/{max_retries})."
                )
                time.sleep(backoff_time)
        if not success:
            raise RuntimeError(
                f"Error getting albums from Spotify for artist {spotify_artist_id}. Failed after {max_retries} attempts."
            )

        all_album_items.extend(data.get("items", []))

        next_url = data.get("next")
        if not next_url:
            return all_album_items
        page_url, page_params = next_url, None


def process_albums_from_spotify(artist, token):
    """Processes the albums for a given artist from the spotify api"""
    try:
        album_list = []
        all_album_items = get_albums_from_spotify(artist["spotify_artist_id"], token)
        for album in all_album_items:
            individual_album = {}
            individual_album["spotify_album_id"] = album["id"]
            individual_album["spotify_artist_id"] = artist["spotify_artist_id"]
            individual_album["album"] = album["name"]
            individual_album["artist"] = artist["artist"]
            individual_album["spotify_url"] = album["external_urls"]["spotify"]
            individual_album["type"] = album["album_type"]
            individual_album["release_date"] = album["release_date"]
            individual_album["total_tracks"] = album["total_tracks"]
            individual_album["is_processed"] = False
            individual_album["images"] = album["images"]
            album_list.append(individual_album)
        return album_list
    except Exception as e:
        logger.error(f"Error processing albums from spotify: {e}")
        raise Exception(f"Error processing albums from spotify: {e}")

def write_albums_to_gcs(artists, bucket_name, base_blob_name):
    """Writes the albums to the gcs bucket"""
    try:
        token = get_spotify_access_token()
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        for artist in tqdm(artists):
            blob = bucket.blob(f"{artist['full_blob_name']}/albums.json")
            albums = process_albums_from_spotify(artist, token)
            blob.upload_from_string(
                json.dumps(albums, indent=3, ensure_ascii=False),
                content_type="application/json",
            )
            time.sleep(0.5)
            logger.info(
                f"Successfully wrote albums for {artist['artist']} to gcs bucket {bucket_name} with blob name {artist['full_blob_name']}/albums.json"
            )
        logger.info(
            f"Successfully wrote albums for {len(artists)} artists to gcs bucket {bucket_name} with base blob name {base_blob_name}"
        )
    except Exception as e:
        logger.error(
            f"Error writing albums to gcs bucket {bucket_name} with base blob name {base_blob_name}: {e}"
        )
        raise Exception(
            f"Error writing albums to gcs bucket {bucket_name} with base blob name {base_blob_name}: {e}"
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
    
    write_albums_to_gcs(
        artists,
        "music-ml-data",
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}",
    )
