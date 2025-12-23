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

from ingestion.utils import (
    get_artists_from_gcs,
    get_albums_from_gcs,
    get_artist_songs_from_gcs,
)

fs = gcsfs.GCSFileSystem()
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)

BUCKET_NAME = "music--data"


def create_artists_metadata_parquet(artists, bucket_name, base_blob_name):
    """Create and upload artists.parquet with calculated artist popularity."""
    try:
        df = pd.json_normalize(artists)

        initial_count = len(df)
        df = add_artist_popularity(df)

        df.drop(columns=["full_blob_name"], errors="ignore", inplace=True)
        df = df.drop_duplicates(subset=["spotify_artist_id"])
        duplicates_dropped = initial_count - len(df)
        final_count = len(df)

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{base_blob_name}/artists.parquet")
        blob.upload_from_file(buffer, content_type="application/parquet")
        logger.info(f"Artists stats: {initial_count} initial, {duplicates_dropped} duplicates dropped, {final_count} final.")
        logger.info(f"Successfully wrote artists metadata parquet to gcs bucket {bucket_name} with blob name {base_blob_name}/artists.parquet.")
    except Exception as e:
        logger.error(f"Error creating artists metadata parquet: {e}")
        raise


def create_albums_metadata_parquet(artists, bucket_name, base_blob_name):
    """Create and upload albums.parquet from artists' album JSON."""
    try:
        albums = []
        for artist in tqdm(artists, ncols=100, leave=True):
            artist_albums = get_albums_from_gcs(artist, bucket_name)
            albums.extend(artist_albums)

        initial_count = len(albums)
        df = pd.json_normalize(albums)

        df.drop(columns=["origination_artist_id"], errors="ignore", inplace=True)
        df = df.drop_duplicates(subset=["spotify_album_id"])
        duplicates_dropped = initial_count - len(df)
        final_count = len(df)

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{base_blob_name}/albums.parquet")
        blob.upload_from_file(buffer, content_type="application/parquet")
        logger.info(f"Albums stats: {initial_count} initial, {duplicates_dropped} duplicates dropped, {final_count} final.")
        logger.info(f"Successfully wrote albums metadata parquet to gcs bucket {bucket_name} with blob name {base_blob_name}/albums.parquet.")
    except Exception as e:
        logger.error(f"Error creating albums metadata parquet: {e}")
        raise


def create_songs_metadata_parquet(artists, bucket_name, base_blob_name):
    """Create and upload songs.parquet with adjusted popularity columns."""
    try:
        songs = []
        for artist in tqdm(artists, ncols=100, leave=True):
            artist_songs = get_artist_songs_from_gcs(artist, bucket_name)
            songs.extend(artist_songs)

        initial_count = len(songs)
        df = pd.json_normalize(songs)

        zero_streams_count = (df["total_streams"] == 0).sum()
        df = add_song_popularity(df)
        df = override_song_popularity(df)
        df = add_spotify_song_popularity(df)

        df.drop(columns=["spotify_popularity"], errors="ignore", inplace=True)
        df = df.drop_duplicates(subset=["spotify_song_id"])
        duplicates_dropped = initial_count - len(df)
        final_count = len(df)

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{base_blob_name}/songs.parquet")
        blob.upload_from_file(buffer, content_type="application/parquet")
        logger.info(f"Songs stats: {initial_count} initial, {zero_streams_count} songs with zero streams (used Spotify popularity), {duplicates_dropped} duplicates dropped, {final_count} final.")
        logger.info(f"Successfully wrote songs metadata parquet to gcs bucket {bucket_name} with blob name {base_blob_name}/songs.parquet.")
    except Exception as e:
        logger.error(f"Error creating songs metadata parquet: {e}")
        raise


def add_artist_popularity(df):
    """Compute artist popularity from follower count using an interpolation table."""
    try:
        FOLLOWERS = np.array(
            [
                0,
                1_000,
                50_000,
                250_000,
                1_000_000,
                5_000_000,
                10_000_000,
                15_000_000,
                25_000_000,
                50_000_000,
            ],
            dtype=float,
        )

        POPULARITY = np.array([0, 10, 25, 40, 59, 75, 85, 90, 95, 100], dtype=float)

        df["popularity"] = np.nan

        followers_filled = df["followers"].fillna(0)
        clipped_followers = followers_filled.clip(
            FOLLOWERS.min(), FOLLOWERS.max()
        ).to_numpy()

        df["popularity"] = np.interp(clipped_followers, FOLLOWERS, POPULARITY)
        df["popularity"] = df["popularity"].round().astype("Int64")

        return df
    except Exception as e:
        logger.error(f"Error adding artist popularity: {e}")
        raise


def add_song_popularity(df):
    """Compute song popularity from total_streams using an interpolation table."""
    try:
        STREAMS = np.array(
            [
                0,
                25_000_000,
                50_000_000,
                100_000_000,
                150_000_000,
                250_000_000,
                500_000_000,
                1_000_000_000,
                2_000_000_000,
            ],
            dtype=float,
        )

        POPULARITY = np.array([0, 38, 53, 61, 68, 75, 84, 95, 100], dtype=float)

        df["popularity"] = np.nan

        non_zero_streams = df["total_streams"] > 0
        clipped_streams = (
            df.loc[non_zero_streams, "total_streams"]
            .clip(STREAMS.min(), STREAMS.max())
            .to_numpy()
        )
        df.loc[non_zero_streams, "popularity"] = np.interp(
            clipped_streams, STREAMS, POPULARITY
        )
        df["popularity"] = df["popularity"].round().astype("Int64")
        return df
    except Exception as e:
        logger.error(f"Error adding song popularity: {e}")
        raise


def override_song_popularity(df, threshold=45):
    """For clear outliers, override derived popularity with Spotify's popularity."""
    try:
        error_songs = (df["total_streams"] > 0) & (
            df["spotify_popularity"] > df["popularity"] + threshold
        )
        df.loc[error_songs, "popularity"] = df.loc[error_songs, "spotify_popularity"]

        logger.info(f"Overridden {error_songs.sum()} error songs popularity.")
        return df
    except Exception as e:
        logger.error(f"Error overriding song popularity: {e}")
        raise


def add_spotify_song_popularity(df):
    """For songs with zero streams, use Spotify popularity directly."""
    try:
        songs_with_zero_streams = df["total_streams"] == 0
        count = songs_with_zero_streams.sum()
        df.loc[songs_with_zero_streams, "popularity"] = df.loc[
            songs_with_zero_streams, "spotify_popularity"
        ]
        if count > 0:
            logger.info(f"Applied Spotify popularity to {count} songs with zero streams.")
        return df
    except Exception as e:
        logger.error(f"Error adding Spotify song popularity: {e}")
        raise


def read_parquet():
    """Debug helper to read a songs parquet file for a specific page/batch."""
    df = pd.read_parquet(
        f"gs://music--data/parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}/albums.parquet",
        filesystem=fs,
    )
    print(len(df[df["release_precision"] == "month"]))
    print(df[df["release_precision"] == "month"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--page_number", type=int, default=1)
    parser.add_argument("--batch_number", type=int, default=1)
    args = parser.parse_args()

    try:
        artists = get_artists_from_gcs(
            BUCKET_NAME,
            f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
        )
        create_artists_metadata_parquet(
            artists,
            BUCKET_NAME,
            f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}",
        )
        create_albums_metadata_parquet(
            artists,
            BUCKET_NAME,
            f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}",
        )
        create_songs_metadata_parquet(
            artists,
            BUCKET_NAME,
            f"parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}",
        )
        # read_parquet()
    except Exception as e:
        logger.error(f"Error creating parquet: {e}")
        raise
