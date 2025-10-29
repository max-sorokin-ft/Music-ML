import pandas as pd
import argparse
from io import BytesIO
from google.cloud import storage
import logging
import json
import gcsfs
from tqdm import tqdm
import numpy as np

from scripts.utils.gcs_utils import (
    get_artists_from_gcs,
    get_albums_from_gcs,
    get_artist_songs_from_gcs,
)

fs = gcsfs.GCSFileSystem(token="gcp_creds.json")
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)

BUCKET_NAME = "music-ml-data"

def create_artists_metadata_parquet(artists, bucket_name, base_blob_name):
    df = pd.json_normalize(artists)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client = storage.Client.from_service_account_json("gcp_creds.json")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{base_blob_name}/artists.parquet")
    blob.upload_from_file(buffer, content_type="application/parquet")

def create_albums_metadata_parquet(artists, bucket_name, base_blob_name):
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

def create_songs_metadata_parquet(artists, bucket_name, base_blob_name):
    songs = []
    for artist in tqdm(artists):
        artist_songs = get_artist_songs_from_gcs(artist, bucket_name)
        songs.extend(artist_songs)
    
    df = pd.json_normalize(songs)
    df = add_popularity_from_streams(df)
    buffer = BytesIO
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client = storage.Client.from_service_account_json("gcp_creds.json")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{base_blob_name}/songs.parquet")
    blob.upload_from_file(buffer, content_type="application/parquet")

def add_popularity_from_streams(df):
    """
    Adds a 'popularity' column to the DataFrame based on total_streams.
    Songs with 0 or missing streams get popularity = 0.
    Others are linearly interpolated between fixed anchor points.
    """

    # Define anchor points (streams → popularity)
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

    df["popularity"] = 0

    non_zero_streams = df["total_streams"] > 0

    clipped_streams = df.loc[non_zero_streams, "total_streams"].clip(STREAMS.min(), STREAMS.max()).to_numpy()
    df.loc[non_zero_streams, "popularity"] = np.interp(clipped_streams, STREAMS, POPULARITY)

    df["popularity"] = df["popularity"].round().astype("Int64")

    return df
    




def read_parquet():
    df = pd.read_parquet(f"gs://music-ml-data/parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.parquet", filesystem=fs)
    print(df.columns)
    print(df.info())


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
    # create_artists_metadata_parquet(artists, BUCKET_NAME, f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}")
    # create_albums_metadata_parquet(artists, BUCKET_NAME, f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}")
    read_parquet()

