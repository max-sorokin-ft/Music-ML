from google.cloud import storage
import requests
import json
import logging
import time
from tqdm import tqdm
import argparse

from auth import get_spotify_access_token
from ingestion.utils import get_artists_from_gcs, normalize_release_date

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

BUCKET_NAME = "music--data"


def fetch_albums_spotify(spotify_artist_id, token, max_retries=2, sleep_time=1):
    """Gets the albums from the spotify api for a given artist"""
    url = f"https://api.spotify.com/v1/artists/{spotify_artist_id}/albums"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"limit": 50, "include_groups": "album,single", "market": "US"}

    all_album_items = []
    page_url, page_params = url, params

    while True:
        success = False
        last_exception = None
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    page_url, headers=headers, params=page_params, timeout=10
                )
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    logger.warning(
                        f"Rate limited by Spotify. Come back in {retry_after} seconds."
                    )

                response.raise_for_status()
                data = response.json()
                success = True
                break
            except Exception as e:
                last_exception = e
                backoff_time = sleep_time * (2**attempt)
                logger.warning(
                    f"Error getting artist's albums page from Spotify: {e}. Retrying in {backoff_time} seconds."
                )
                time.sleep(backoff_time)
        if not success:
            logger.error(
                f"Error getting albums from Spotify for artist {spotify_artist_id}: {last_exception}. Failed after {max_retries} attempts."
            )
            raise last_exception

        all_album_items.extend(data.get("items", []))

        next_url = data.get("next")
        if not next_url:
            return all_album_items
        page_url, page_params = next_url, None


def process_albums_spotify(artist, token):
    """Processes the albums for a given artist from the spotify api"""
    try:
        album_list = []
        all_album_items = fetch_albums_spotify(artist["spotify_artist_id"], token)
        for album in all_album_items:
            individual_album = {}
            individual_album["spotify_album_id"] = album["id"]
            individual_album["album"] = album["name"]
            individual_album["origination_artist_id"] = artist["spotify_artist_id"]
            individual_album["artists"] = [
                artist["name"] for artist in album["artists"]
            ]
            individual_album["spotify_artist_ids"] = [
                artist["id"] for artist in album["artists"]
            ]
            individual_album["album_type"] = album["album_type"]
            individual_album["release_date_precision"] = album["release_date_precision"]
            release_date = album["release_date"]
            individual_album["release_date"] = normalize_release_date(release_date, album["release_date_precision"])
            individual_album["total_tracks"] = album["total_tracks"]
            individual_album["images"] = [image["url"] for image in album["images"]]
            album_list.append(individual_album)
        return album_list
    except Exception as e:
        logger.error(f"Error processing albums from spotify: {e}")
        raise

def dedupe_albums(albums):
    """Deduplicates the albums based on DB existance; primarily for inter group deduplication"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""SELECT spotify_album_id FROM albums""")
        albums_ids = cursor.fetchall()
        albums_ids = [album_id[0] for album_id in albums_ids]
        deduped_albums = [album for album in albums if album["spotify_album_id"] not in albums_ids]
        return deduped_albums
    except Exception as e:
        logger.error(f"Error deduplicating albums: {e}")
        raise

def write_albums_gcs(artists, bucket_name, base_blob_name):
    """Writes the albums to the gcs bucket"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        token = get_spotify_access_token(args.num)
        for artist in tqdm(artists):
            blob = bucket.blob(f"{artist['full_blob_name']}/albums.json")
            albums = process_albums_spotify(artist, token)
            albums = dedupe_albums(albums)
            if albums:
                blob.upload_from_string(
                    json.dumps(albums, indent=3, ensure_ascii=False),
                    content_type="application/json",
                )
                time.sleep(0.5)
                logger.info(
                    f"Successfully wrote {len(albums)} albums for {artist['artist']} to gcs bucket {bucket_name} with blob name {artist['full_blob_name']}/albums.json"
                )
            else:
                logger.info(f"All albums for {artist['origination_artist_id']} already exist in the database.")
        logger.info(
            f"Successfully wrote albums for {len(artists)} artists to gcs bucket {bucket_name} with base blob name {base_blob_name}"
        )
    except Exception as e:
        logger.error(
            f"Error writing albums to gcs bucket {bucket_name} with base blob name {base_blob_name}: {e}"
        )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--page_number", type=int, default=1)
    parser.add_argument( "--batch_number", type=int, default=1)
    parser.add_argument("--num", type=int, default=1)
    args = parser.parse_args()
    
    try:
        artists = get_artists_from_gcs(
            BUCKET_NAME,
            f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
        )

        write_albums_gcs(
            artists,
            BUCKET_NAME,
            f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}",
        )
    except Exception as e:
        logger.error(f"Error running the script get_albums.py: {e}")
        raise
