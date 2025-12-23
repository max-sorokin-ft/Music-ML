from google.cloud import storage
import requests
import json
import logging
import time
from tqdm import tqdm
import argparse

from ingestion.utils import (
    get_artists_from_gcs,
    get_artist_songs_from_gcs,
)
from auth import get_spotify_access_token

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

BUCKET_NAME = "music--data"

# Popularity Adjustment Tables
ADJUSTMENTS_DOWN = [
    ((46, 50), -10),
    ((41, 45), -9),
    ((36, 40), -8),
    ((31, 35), -7),
    ((26, 30), -6),
    ((21, 25), -5),
    ((16, 20), -4),
    ((11, 15), -3),
    ((6, 10), -2),
    ((1, 5), -1),
]

ADJUSTMENTS_UP = [
    ((63, 70), +4),
    ((71, 100), +7),
]


def adjust_spotify_popularity_value(popularity):
    """Helper function to adjust the popularity value of a song"""
    for (low, high), adj in ADJUSTMENTS_DOWN:
        if low <= popularity <= high:
            return popularity + adj

    for (low, high), adj in ADJUSTMENTS_UP:
        if low <= popularity <= high:
            return popularity + adj

    return popularity


def fetch_songs_spotify(songs, token, max_retries=3, sleep_time=1):
    """Fetches songs from the spotify api"""
    last_exception = None
    for attempt in range(max_retries):
        try:
            url = "https://api.spotify.com/v1/tracks"
            headers = {"Authorization": f"Bearer {token}"}
            song_ids = ",".join([song["spotify_song_id"] for song in songs])
            params = {"ids": song_ids}
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                logger.warning(
                    f"Rate limited by Spotify. Come back in {retry_after} seconds."
                )

            response.raise_for_status()
            return response.json()
        except Exception as e:
            last_exception = e
            backoff_time = sleep_time * (2**attempt)
            logger.warning(
                f"Error fetching ISRC from Spotify API: {e}. Retrying in {backoff_time} seconds."
            )
            time.sleep(backoff_time)

    logger.error(
        f"Error fetching ISRC from Spotify API: {last_exception}. Failed after {max_retries} attempts."
    )
    raise last_exception


def process_songs_spotify(songs, token, batch_size=50):
    """Processes the songs, specifically the ISRC and popularity, by batch"""
    try:
        valid_songs = []
        for i in range(0, len(songs), batch_size):
            batch_songs = songs[i : i + batch_size]
            response = fetch_songs_spotify(batch_songs, token)

            for index, song in enumerate(batch_songs):
                track = response["tracks"][index]
                if not track.get("external_ids"):
                    continue

                song["isrc"] = track["external_ids"]["isrc"]
                raw_popularity = track["popularity"]
                adjusted_popularity = adjust_spotify_popularity_value(raw_popularity)
                song["spotify_popularity"] = adjusted_popularity
                valid_songs.append(song)

            time.sleep(0.8)
        return valid_songs
    except Exception as e:
        logger.error(f"Error processing ISRC from Spotify API: {e}")
        raise


def write_isrc_pop_gcs(artists, bucket_name, base_blob_name, token):
    """Writes/adds the ISRC and popularity of the songs to the gcs bucket"""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    try:
        for artist in tqdm(artists):
            songs = get_artist_songs_from_gcs(artist, bucket_name)
            songs = process_songs_spotify(songs, token)
            blob = bucket.blob(f"{artist['full_blob_name']}/songs.json")
            blob.upload_from_string(
                json.dumps(songs, indent=3, ensure_ascii=False),
                content_type="application/json",
            )
            logger.info(
                f"Successfully wrote ISRC for artist {artist['artist']} {len(songs)} songs to gcs bucket {bucket_name} with blob name {artist['full_blob_name']}/songs.json"
            )

        logger.info(
            f"Successfully wrote ISRC for {len(artists)} artists songs to gcs bucket {bucket_name} with blob name {base_blob_name}"
        )
    except Exception as e:
        logger.error(
            f"Error writing ISRC to GCS bucket {bucket_name} with blob name {base_blob_name}: {e}"
        )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--page_number", type=int, default=1)
    parser.add_argument("--batch_number", type=int, default=1)
    parser.add_argument("--num", type=int, default=3)
    args = parser.parse_args()

    try:
        token = get_spotify_access_token(args.num)
        artists = get_artists_from_gcs(
            BUCKET_NAME,
            f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
        )
        write_isrc_pop_gcs(
            artists,
            BUCKET_NAME,
            f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}",
            token,
        )
    except Exception as e:
        logger.error(f"Error running the script get_isrc.py: {e}")
        raise
