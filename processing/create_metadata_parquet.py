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
        df = override_error_songs_popularity(df)
        df = add_spotify_popularity(df)
        df.drop(columns=['spotify_popularity'], errors='ignore', inplace=True)

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
    try:
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

def override_error_songs_popularity(df, threshold=20):
    try:
        error_songs = (df["total_streams"] > 0) & (df["spotify_popularity"] > df["popularity"] + threshold)
        df.loc[error_songs, "popularity"] = df.loc[error_songs, "spotify_popularity"]

        logger.info(f"Overridden {error_songs.sum()} error songs popularity.")
        return df
    except Exception as e:
        logger.error(f"Error overriding error songs popularity: {e}")
        raise

def add_spotify_popularity(df):
    try:
        songs_with_zero_streams = df["total_streams"] == 0
        df.loc[songs_with_zero_streams, "popularity"] = df.loc[songs_with_zero_streams, "spotify_popularity"]
        return df
    except Exception as e:
        logger.error(f"Error adding Spotify popularity: {e}")
        raise

def read_parquet():
    df = pd.read_parquet(f"gs://music-ml-data/parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}/songs.parquet", filesystem=fs)
    print(len(df))
    print(len(df[df["popularity"] > 0]))
    print(len(df[df["total_streams"] > 0]))
    print(df.columns)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--page_number", type=int, default=1)
    parser.add_argument("--batch_number", type=int, default=1)
    args = parser.parse_args()

    artists = get_artists_from_gcs(
        BUCKET_NAME,
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
    )
    create_artists_metadata_parquet(artists, BUCKET_NAME, f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}")
    create_albums_metadata_parquet(artists, BUCKET_NAME, f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}")
    create_songs_metadata_parquet(artists, BUCKET_NAME, f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}")
    # read_parquet()
