from google.cloud import storage
from auth import get_spotify_access_token
import requests
import json
import logging
import time
from tqdm import tqdm
import argparse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

"""
    This script is part of the data acquisition pipeline for the project and it is used to get the albums for a given artist from the spotify api.
    It loops through the artists from a given kworb page and gets the albums for each artist from the spotify api.
    The json data is uploaded to a gcs bucket.
"""


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


def get_all_artist_songs_from_gcs(artist, bucket_name):
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


def fetch_album_songs_from_spotify(album_id, token, max_retries=3, sleep_time=1):
    """Gets the songs from the spotify api"""
    for attempt in range(max_retries):
        try:
            url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"limit": 50}
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            backoff_time = sleep_time * (2**attempt)
            logger.warning(
                f"Error getting album songs from spotify: {e}. Retrying in {backoff_time} seconds."
            )
            time.sleep(backoff_time)
    logger.error(f"Error getting album songs from spotify: {e}")
    raise Exception(f"Error getting album songs from spotify: {e}")


def fetch_artist_top_tracks_from_spotify(artist_id, token, max_retries=3, sleep_time=1):
    """Gets the top tracks from the spotify api for a given artist"""
    for attempt in range(max_retries):
        try:
            url = f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks"
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            backoff_time = sleep_time * (2**attempt)
            logger.warning(
                f"Error getting artist top tracks from spotify: {e}. Retrying in {backoff_time} seconds."
            )
            time.sleep(backoff_time)
    logger.error(f"Error getting artist top tracks from spotify: {e}")
    raise Exception(f"Error getting artist top tracks from spotify: {e}")


def process_album_songs_from_spotify(album, token):
    """Processes the songs from the spotify api for a given album"""
    try:
        songs_list = []
        songs = fetch_album_songs_from_spotify(album["spotify_album_id"], token)
        for song in songs["items"]:
            individual_song = {}
            individual_song["spotify_song_id"] = song["id"]
            individual_song["spotify_album_id"] = album["spotify_album_id"]
            individual_song["spotify_artist_id"] = album["spotify_artist_id"]
            individual_song["name"] = song["name"]
            individual_song["album"] = album["album"]
            individual_song["artists"] = [artist["name"] for artist in song["artists"]]
            individual_song["primary_artist"] = song["artists"][0]["name"]
            individual_song["spotify_url"] = song["external_urls"]["spotify"]
            individual_song["release_date"] = album["release_date"]
            individual_song["duration_ms"] = song["duration_ms"]
            individual_song["explicit"] = song["explicit"]
            individual_song["images"] = album["images"]
            songs_list.append(individual_song)
        return songs_list
    except Exception as e:
        logger.error(f"Error processing album songs from spotify: {e}")
        raise Exception(f"Error processing album songs from spotify: {e}")


def process_artist_top_tracks_from_spotify(artist, token, top_n_tracks=10):
    """Processes the top tracks from the spotify api for a given artist, only includes singles"""
    try:
        top_songs = []
        top_tracks = fetch_artist_top_tracks_from_spotify(
            artist["spotify_artist_id"], token
        )
        for track in top_tracks["tracks"][:top_n_tracks]:
                individual_song = {}
                individual_song["spotify_song_id"] = track["id"]
                individual_song["spotify_album_id"] = track["album"]["id"]
                individual_song["spotify_artist_id"] = artist["spotify_artist_id"]
                individual_song["name"] = track["name"]
                individual_song["album"] = track["album"]["name"]
                individual_song["artists"] = [
                    artist["name"] for artist in track["artists"]
                ]
                individual_song["primary_artist"] = track["artists"][0]["name"]
                individual_song["spotify_url"] = track["external_urls"]["spotify"]
                individual_song["release_date"] = track["album"]["release_date"]
                individual_song["duration_ms"] = track["duration_ms"]
                individual_song["explicit"] = track["explicit"]
                individual_song["images"] = track["album"]["images"]
                top_songs.append(individual_song)
        return top_songs
    except Exception as e:
        logger.error(f"Error processing artist top tracks from spotify: {e}")
        raise Exception(f"Error processing artist top tracks from spotify: {e}")


def dedupe_single_songs(artist, token, bucket_name):
    """Dedupe the songs. This removes songs that are already in the albums, and leaves singles
    in the top 10 songs of the artist."""
    try:
        deduped_songs = []
        top_songs = process_artist_top_tracks_from_spotify(artist, token)
        all_album_songs = get_all_artist_songs_from_gcs(artist, bucket_name)
        for song in top_songs:
            if song["spotify_song_id"] not in [
                song["spotify_song_id"] for song in all_album_songs
            ]:
                deduped_songs.append(song)
        logger.info(f"Successfully deduped {len(deduped_songs)} single songs for artist {artist['artist']}")
        return deduped_songs
    except Exception as e:
        logger.error(f"Error deduping single songs: {e}")
        raise Exception(f"Error deduping single songs: {e}")


def write_album_songs_to_gcs(artists, bucket_name, base_blob_name):
    """Writes the songs from an album for an aritst inside the album's folder"""
    try:
        token = get_spotify_access_token()
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        for artist in tqdm(artists):
            albums = get_albums_from_gcs(artist, bucket_name)
            all_album_songs = []
            for album in albums:
                if album["type"] == "album":
                    blob = bucket.blob(
                        f"{artist['full_blob_name']}/{album['spotify_album_id']}/songs.json"
                    )
                    songs = process_album_songs_from_spotify(album, token)
                    blob.upload_from_string(
                        json.dumps(songs, indent=3, ensure_ascii=False),
                        content_type="application/json",
                    )
                    all_album_songs.extend(songs)
                    time.sleep(0.5)
            logger.info(
                f"Successfully wrote albums' songs for {len(albums)} albums for artist {artist['artist']} to gcs bucket {bucket_name} with blob name {base_blob_name} and seperate folders for each album."
            )
            blob = bucket.blob(f"{artist['full_blob_name']}/songs.json")
            blob.upload_from_string(
                json.dumps(all_album_songs, indent=3, ensure_ascii=False),
                content_type="application/json",
            )
            logger.info(
                f"Successfully wrote {len(all_album_songs)} albums' songs for {artist['artist']} to gcs bucket {bucket_name} with blob name {base_blob_name}/songs.json"
            )
        logger.info(
            f"Successfully wrote albums' songs for {len(artists)} artists to gcs bucket {bucket_name} with blob name {base_blob_name}"
        )
    except Exception as e:
        logger.error(
            f"Error writing songs to gcs bucket {bucket_name} with blob name {base_blob_name}: {e}"
        )
        raise Exception(
            f"Error writing songs to gcs bucket {bucket_name} with blob name {base_blob_name}: {e}"
        )


def write_single_songs_to_gcs(artists, bucket_name, base_blob_name):
    """Writes the single songs to the album's folder"""
    try:
        token = get_spotify_access_token()
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)

        for artist in tqdm(artists):
            single_songs = dedupe_single_songs(artist, token, bucket_name)
            for song in single_songs:
                blob = bucket.blob(
                    f"{artist['full_blob_name']}/{song['spotify_album_id']}/songs.json"
                )
                blob.upload_from_string(
                    json.dumps(single_songs, indent=3, ensure_ascii=False),
                    content_type="application/json",
                )
            logger.info(
                f"Successfully wrote {len(single_songs)} single songs for artist {artist['artist']} to gcs bucket {bucket_name} with blob name {base_blob_name} and seperate folders for each single."
            )
            blob = bucket.blob(f"{artist['full_blob_name']}/songs.json")
            existing_songs = json.loads(blob.download_as_string())
            existing_songs.extend(single_songs)
            blob.upload_from_string(
                json.dumps(existing_songs, indent=3, ensure_ascii=False),
                content_type="application/json",
            )
            logger.info(
                f"Successfully added {len(single_songs)} single songs to {len(existing_songs)} existing songs for artist {artist['artist']}"
            )
            time.sleep(0.5)
        logger.info(
            f"Successfully wrote single songs for {len(artists)} artists to gcs bucket {bucket_name} with blob name {base_blob_name}"
        )
    except Exception as e:
        logger.error(
            f"Error writing single songs to gcs bucket {bucket_name} with blob name {base_blob_name}: {e}"
        )
        raise Exception(
            f"Error writing single songs to gcs bucket {bucket_name} with blob name {base_blob_name}: {e}"
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

    write_album_songs_to_gcs(
        artists,
        "music-ml-data",
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}",
    )
    write_single_songs_to_gcs(
        artists,
        "music-ml-data",
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}",
    )
