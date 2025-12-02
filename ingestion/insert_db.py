import logging
from psycopg2.extras import execute_values
from db.db import get_connection
import argparse
import pandas as pd
import gcsfs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

"""This script inserts artists and albums into the database based on their per batch parquet files"""

fs = gcsfs.GCSFileSystem()


def get_artists_parquet():
    """Read artists.parquet and return rows mapped to DB columns."""
    df = pd.read_parquet(
        f"gs://music--data/parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.parquet",
        filesystem=fs,
    )
    rows = [
        (
            row.spotify_artist_id,
            row.artist,
            int(row.monthly_listeners),
            int(row.followers),
            int(row.popularity),
            list(row.genres),
            list(row.images),
        )
        for row in df.itertuples(index=False)
    ]
    return rows


def get_albums_parquet():
    """Read albums.parquet and return rows mapped to DB columns."""
    df = pd.read_parquet(
        f"gs://music--data/parquet_metadata/artists_kworbpage{args.page_number}/batch{args.batch_number}/albums.parquet",
        filesystem=fs,
    )
    rows = [
        (
            row.spotify_album_id,
            row.album,
            list(row.artists),
            list(row.spotify_artist_ids),
            row.album_type,
            row.release_date,
            row.release_date_precision,
            int(row.total_tracks),
            list(row.images),
        )
        for row in df.itertuples(index=False)
    ]
    return rows


def insert_artists():
    """Bulk insert artist rows into the artists table."""
    try:
        conn = get_connection()
        cur = conn.cursor()

        rows = get_artists_parquet()

        query = """
            INSERT INTO artists (spotify_artist_id, artist, monthly_listeners, followers, popularity, genres, images)
            VALUES %s
        """
        execute_values(cur, query, rows)
        conn.commit()
        logger.info(f"Successfully inserted {len(rows)} artists into the artists table.")
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error inserting artists: {e}")
        raise


def insert_albums():
    """Bulk insert album rows into the albums table."""
    try:
        conn = get_connection()
        cur = conn.cursor()

        rows = get_albums_parquet()

        query = """
            INSERT INTO albums (spotify_album_id, album, artists, spotify_artist_ids, album_type, release_date, release_date_precision, total_tracks, images)
            VALUES %s
        """
        execute_values(cur, query, rows)
        conn.commit()
        logger.info(f"Successfully inserted {len(rows)} albums into the albums table.")
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error inserting albums: {e}")
        # Find the problematic row
        for row in rows:
            release_date = row[5]  # release_date is 6th element (0-indexed: 5)
            album_id = row[0]
            if len(str(release_date)) == 4:
                print(f"Problematic album ID: {album_id}, Release Date: {release_date}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--page_number", type=int, default=1)
    parser.add_argument("--batch_number", type=int, default=1)
    args = parser.parse_args()

    insert_artists()
    insert_albums()
