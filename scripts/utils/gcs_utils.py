from google.cloud import storage
import json
import logging

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

def get_albums_from_gcs(artist, bucket_name):
    """Gets the albums from the gcs bucket for a given artist"""
    try:
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob_name = f"{artist['full_blob_name']}/albums.json"
        blob = bucket.blob(blob_name)
        albums = json.loads(blob.download_as_string())
        return albums
    except Exception as e:
        logger.error(
            f"Error getting albums from gcs bucket {bucket_name} with blob name {blob_name}: {e}"
        )
        raise Exception(
            f"Error getting albums from gcs bucket {bucket_name} with blob name {blob_name}: {e}"
        )

def get_artist_songs_from_gcs(artist, bucket_name):
    """Gets all the songs combined from the gcs bucket for a given artist"""
    try:
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{artist['full_blob_name']}/songs.json")
        return json.loads(blob.download_as_string())
    except Exception as e:
        logger.error(
            f"Error getting all artist songs from gcs bucket {bucket_name} with blob name {artist['full_blob_name']}/songs.json: {e}"
        )
        raise Exception(
            f"Error getting all artist songs from gcs bucket {bucket_name} with blob name {artist['full_blob_name']}/songs.json: {e}"
        )

def get_artist_grouped_songs_from_gcs(artist, bucket_name):
    """Gets all the songs combined from the gcs bucket for a given artist"""
    try:
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{artist['full_blob_name']}/grouped_songs.json")
        return json.loads(blob.download_as_string())
    except Exception as e:
        logger.error(
            f"Error getting all artist songs from gcs bucket {bucket_name} with blob name {artist['full_blob_name']}/songs.json: {e}"
        )
        raise Exception(
            f"Error getting all artist songs from gcs bucket {bucket_name} with blob name {artist['full_blob_name']}/songs.json: {e}"
        )
