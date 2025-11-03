import pandas as pd
import argparse
from io import BytesIO
from google.cloud import storage
import logging
import json
import gcsfs
from tqdm import tqdm
import numpy as np
import requests
from auth import get_spotify_access_token
import logging
import time

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)

from storage.gcs_utils import (
    get_artists_from_gcs,
    get_albums_from_gcs,
    get_artist_songs_from_gcs,
)

fs = gcsfs.GCSFileSystem(token="gcp_creds.json")
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)

BUCKET_NAME = "music-ml-data"

def create_artists_metadata_parquet(artists, bucket_name, base_blob_name):
    try:
        df = pd.json_normalize(artists)

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{base_blob_name}/artists.parquet")
        blob.upload_from_file(buffer, content_type="application/parquet")
        logger.info(f"Successfully wrote artists metadata parquet for {len(artists)} artists to gcs bucket {bucket_name} with blob name {base_blob_name}/artists.parquet.")
    except Exception as e:
        logger.error(f"Error creating artists metadata parquet: {e}")
        raise

def create_albums_metadata_parquet(artists, bucket_name, base_blob_name):
    try:
        albums = []
        for artist in tqdm(artists):
            artist_albums = get_albums_from_gcs(artist, bucket_name)
            albums.extend(artist_albums)
        
        df = pd.json_normalize(albums)

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{base_blob_name}/albums.parquet")
        blob.upload_from_file(buffer, content_type="application/parquet")
        logger.info(f"Successfully wrote albums metadata parquet for {len(albums)} albums to gcs bucket {bucket_name} with blob name {base_blob_name}/albums.parquet.")
    except Exception as e:
        logger.error(f"Error creating albums metadata parquet: {e}")
        raise

def create_songs_metadata_parquet(artists, bucket_name, base_blob_name):
    try:
        songs = []
        for artist in tqdm(artists):
            artist_songs = get_artist_songs_from_gcs(artist, bucket_name)
            songs.extend(artist_songs)
        
        df = pd.json_normalize(songs)
        df = df.drop(columns=['images'], errors='ignore') 
         
        df = add_popularity_from_streams(df)
        song_id_popularity_map = process_popularity_spotify(df)
        df = add_popularity_to_df(df, song_id_popularity_map)
        df = adjust_spotify_popularity(df)

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{base_blob_name}/songs.parquet")
        blob.upload_from_file(buffer, content_type="application/parquet")
        logger.info(f"Successfully wrote songs metadata parquet for {len(songs)} songs to gcs bucket {bucket_name} with blob name {base_blob_name}/songs.parquet.")
    except Exception as e:
        logger.error(f"Error creating songs metadata parquet: {e}")
        raise

def add_popularity_from_streams(df):
    """
    Adds a 'popularity' column to the DataFrame based on total_streams.
    Songs with 0 or missing streams get popularity = 0.
    Others are linearly interpolated between fixed anchor points.
    """
    try:
        # Anchor points (streams → popularity)
        STREAMS = np.array([
            0,
            25_000_000,
            50_000_000,
            100_000_000,
            150_000_000,
            250_000_000,
            500_000_000,
            1_000_000_000,
            2_000_000_000
        ], dtype=float)

        POPULARITY = np.array([
            0,   # 0 streams
            38,  # 25M
            53,  # 50M
            61,  # 100M
            68,  # 150M
            75,  # 250M
            84,  # 500M
            95,  # 1B
            100  # 2B
        ], dtype=float)

        df["popularity"] = np.nan

        non_zero_streams = df["total_streams"] > 0

        clipped_streams = df.loc[non_zero_streams, "total_streams"].clip(STREAMS.min(), STREAMS.max()).to_numpy()
        df.loc[non_zero_streams, "popularity"] = np.interp(clipped_streams, STREAMS, POPULARITY)

        df["popularity"] = df["popularity"].round().astype("Int64")
        return df
    except Exception as e:
        logger.error(f"Error adding popularity from streams: {e}")
        raise
    

def collect_songs_with_zero_streams(df):
    try:
        song_ids = df.loc[df["total_streams"] == 0, "spotify_song_id"].tolist()
        return song_ids
    except Exception as e:
        logger.error(f"Error collecting songs with zero streams: {e}")
        raise

def fetch_popularity_spotify(song_ids, token, max_retries=2, sleep_time=1):
    last_exception = None
    for attempt in range(max_retries):
        try:
            url = f"https://api.spotify.com/v1/tracks"

            song_ids_str = ",".join(song_ids)
            params = {
                "ids": song_ids_str
            }
            headers = {
                "Authorization": f"Bearer {token}"
            }
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                logger.warning(f"Rate limited by Spotify. Come back in {retry_after} seconds.")

            response.raise_for_status()
            return response.json()
        except Exception as e:
            last_exception = e
            backoff_time = sleep_time * (2**attempt)
            logger.warning(f"Error fetching popularity from Spotify: {e}. Retrying in {backoff_time} seconds.")
            time.sleep(backoff_time)
        logger.error(f"Error fetching popularity from Spotify: {last_exception}. Failed after {max_retries} attempts.")
        raise last_exception

def process_popularity_spotify(df, batch_size=50):
    token = get_spotify_access_token()
    try:
        song_id_popularity_map = {}
        song_ids = collect_songs_with_zero_streams(df)

        for i in tqdm(range(0, len(song_ids), batch_size)):
            batch_song_ids = song_ids[i: i + batch_size]
            response = fetch_popularity_spotify(batch_song_ids, token)
            for index, id in enumerate(batch_song_ids):
                song_id_popularity_map[id] = response["tracks"][index]["popularity"]
        return song_id_popularity_map
    except Exception as e:
        logger.error(f"Error processing popularity from Spotify: {e}")
        raise

def add_popularity_to_df(df, song_id_popularity_map):
    df["popularity"] = df["popularity"].fillna(df["spotify_song_id"].map(song_id_popularity_map))
    return df

def adjust_spotify_popularity(df):
    df.loc[df["popularity"] <= 50, "popularity"] -= 10
    df.loc[df["popularity"] > 50, "popularity"] += 10

    df["popularity"] = df["popularity"].clip(0, 100)

    return df

def read_parquet():
    df = pd.read_parquet(f"gs://music-ml-data/parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}/songs.parquet", filesystem=fs)
    # oasis_df = df[df["spotify_artist_id"] == "2DaxqgrOhkeH0fpeiQq2f4"]
    # print(len(oasis_df[oasis_df["total_streams"] == 0]))
    # print(len(oasis_df[oasis_df["total_streams"] > 0]))
    # print(oasis_df["total_streams"].mean())
    # print(oasis_df["total_streams"].median())
    print(df.loc[df["song"] == "Miss Atomic Bomb"])

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
    create_artists_metadata_parquet(artists, BUCKET_NAME, f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}")
    create_albums_metadata_parquet(artists, BUCKET_NAME, f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}")
    create_songs_metadata_parquet(artists, BUCKET_NAME, f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}")

