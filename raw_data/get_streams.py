import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import json
import logging
from google.cloud import storage
import argparse
import time

from storage.gcs_utils import (
    get_artists_from_gcs,
    get_artist_songs_from_gcs,
    get_artist_grouped_songs_from_gcs,
)
from auth import get_spotify_access_token
from processing.group_songs import group_songs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

BASE_URL = "https://kworb.net/spotify/artist/{spotify_artist_id}_songs.html"
BUCKET_NAME = "music-ml-data"

def get_artist_songs_kworb(artist):
    """Gets the html of the page from kworb's page"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = BASE_URL.format(spotify_artist_id=artist["spotify_artist_id"])
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = "utf-8"
        return response.text
    except Exception as e:
        logger.error(
            f"Error getting html for artist {artist['spotify_artist_id']}: {e}"
        )
        raise


def process_artist_songs_kworb(artist):
    """Processes the html and returns dict of {spotify_id: total_streams}"""
    try:
        html = get_artist_songs_kworb(artist)
        soup = BeautifulSoup(html, "lxml")

        table = soup.find("table", class_="addpos sortable")
        tr_list = table.find("tbody").find_all("tr")

        kworb_songs = {}
        for tr in tr_list:
            td_list = tr.find_all("td")
            link = td_list[0].find("a")
            href = link["href"]
            spotify_song_id = href.split("/track/")[-1]
            total_streams = int(td_list[1].text.strip().replace(",", ""))
            kworb_songs[spotify_song_id] = total_streams

        return kworb_songs
    except Exception as e:
        logger.error(
            f"Error processing songs for artist {artist['spotify_artist_id']}: {e}"
        )
        raise


def match_streams_to_grouped_songs(grouped_songs, kworb_songs):
    """Matches kworb streams to grouped songs and assigns streams to all variants in each group"""
    try:
        for song_data in grouped_songs.values():
            streams = None
            for variant in song_data["variants"]:
                if variant["spotify_song_id"] in kworb_songs:
                    streams = kworb_songs[variant["spotify_song_id"]]
                    break

            for variant in song_data["variants"]:
                if streams:
                    variant["total_streams"] = streams
                else:
                    variant["total_streams"] = 0

        return grouped_songs
    except Exception as e:
        logger.error(f"Error matching streams to grouped songs: {e}")
        raise


def collect_missing_ids(grouped_songs, kworb_songs):
    """Collects songs from Kworb that don't exist in grouped songs"""
    try:
        existing_ids = set()
        for song_data in grouped_songs.values():
            for variant in song_data["variants"]:
                existing_ids.add(variant["spotify_song_id"])

        missing_ids = []
        for kworb_id in kworb_songs.keys():
            if kworb_id not in existing_ids:
                missing_ids.append(kworb_id)

        return missing_ids
    except Exception as e:
        logger.error(f"Error collecting missing IDs: {e}")
        raise


def fetch_tracks_from_spotify(track_ids, token, max_retries=3, sleep_time=1):
    """Fetches tracks from Spotify API in batches of 50"""
    try:
        all_tracks = []
        last_exception = None
        for i in range(0, len(track_ids), 50):
            batch = track_ids[i : i + 50]
            ids_str = ",".join(batch)

            for attempt in range(max_retries):
                try:
                    url = f"https://api.spotify.com/v1/tracks?ids={ids_str}"
                    headers = {"Authorization": f"Bearer {token}"}
                    response = requests.get(url, headers=headers, timeout=10)
                    response.raise_for_status()
                    data = response.json()

                    for track in data["tracks"]:
                        if track:
                            all_tracks.append(track)

                    time.sleep(0.1)
                    break
                except Exception as e:
                    last_exception = e
                    backoff_time = sleep_time * (2**attempt)
                    logger.warning(
                        f"Error fetching tracks batch: {e}. Retrying in {backoff_time} seconds."
                    )
                    time.sleep(backoff_time)

        return all_tracks
    except Exception as e:
        logger.error(f"Error fetching tracks from Spotify: {last_exception}. Failed after {max_retries} attempts.")
        raise


def process_backfilled_tracks(tracks, artist):
    """Processes backfilled tracks from Spotify API to songs.json format with streams"""
    try:
        processed_songs = []

        for track in tracks:
            individual_song = {}
            individual_song["spotify_song_id"] = track["id"]
            individual_song["spotify_album_id"] = track["album"]["id"]
            individual_song["spotify_artist_id"] = artist["spotify_artist_id"]
            individual_song["song"] = track["name"]
            individual_song["album"] = track["album"]["name"]
            individual_song["artists"] = [
                artist_data["name"] for artist_data in track["artists"]
            ]
            individual_song["artist_ids"] = [
                artist_data["id"] for artist_data in track["artists"]
            ]
            individual_song["spotify_url"] = track["external_urls"]["spotify"]
            individual_song["release_date"] = track["album"]["release_date"]
            individual_song["duration_ms"] = track["duration_ms"]
            individual_song["explicit"] = track["explicit"]
            individual_song["images"] = track["album"]["images"]

            processed_songs.append(individual_song)

        return processed_songs
    except Exception as e:
        logger.error(f"Error processing backfilled tracks: {e}")
        raise


def update_songs_from_grouped(songs, grouped_songs):
    """Updates songs list with streams from grouped_songs by mapping IDs"""
    try:
        id_to_streams = {}
        for song_data in grouped_songs.values():
            for variant in song_data["variants"]:
                id_to_streams[variant["spotify_song_id"]] = variant["total_streams"]

        for song in songs:
            song["total_streams"] = id_to_streams[song["spotify_song_id"]]

        return songs
    except Exception as e:
        logger.error(f"Error updating songs from grouped: {e}")
        raise


def write_streams_to_gcs(artists, bucket_name, base_blob_name):
    """Main pipeline: matches streams, backfills missing tracks, writes songs.json and grouped_songs.json"""
    try:
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        token = get_spotify_access_token()

        for artist in tqdm(artists):
            try:
                songs = get_artist_songs_from_gcs(artist, bucket_name)
                kworb_songs = process_artist_songs_kworb(artist)
                grouped_songs = get_artist_grouped_songs_from_gcs(artist, bucket_name)

                grouped_songs = match_streams_to_grouped_songs(
                    grouped_songs, kworb_songs
                )

                missing_ids = collect_missing_ids(grouped_songs, kworb_songs)

                if missing_ids:
                    fetched_tracks = fetch_tracks_from_spotify(missing_ids, token)
                    backfilled_songs = process_backfilled_tracks(
                        fetched_tracks, artist
                    )
                    songs.extend(backfilled_songs)

                    logger.info(
                        f"Added {len(backfilled_songs)} backfilled tracks for {artist['artist']}"
                    )

                    grouped_songs = group_songs(artist, bucket_name, songs)
                    grouped_songs = match_streams_to_grouped_songs(
                        grouped_songs, kworb_songs
                    )

                    songs = update_songs_from_grouped(songs, grouped_songs)

                blob = bucket.blob(f"{artist['full_blob_name']}/songs.json")
                blob.upload_from_string(
                    json.dumps(songs, indent=3, ensure_ascii=False),
                    content_type="application/json",
                )

                blob = bucket.blob(f"{artist['full_blob_name']}/grouped_songs.json")
                blob.upload_from_string(
                    json.dumps(grouped_songs, indent=3, ensure_ascii=False),
                    content_type="application/json",
                )

                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error processing artist {artist['artist']}: {e}")
                continue

        logger.info(
            f"Successfully wrote streams for {len(artists)} artists to gcs bucket {bucket_name} with base blob name {base_blob_name}"
        )
    except Exception as e:
        logger.error(
            f"Error writing streams to gcs bucket {bucket_name} with base blob name {base_blob_name}: {e}"
        )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--page_number", type=int, default=1, help="The number of the kworb's page"
    )
    parser.add_argument(
        "--batch_number", type=int, default=1, help="The batch number of the artists"
    )
    args = parser.parse_args()

    artists = get_artists_from_gcs(
        BUCKET_NAME,
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
    )

    write_streams_to_gcs(
        artists,
        BUCKET_NAME,
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}",
    )
