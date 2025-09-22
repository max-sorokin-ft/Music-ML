import requests
from bs4 import BeautifulSoup
from auth import get_spotify_access_token
from tqdm import tqdm
from datetime import datetime
import json
import time
import logging
from google.cloud import storage
import argparse
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

"""
    This script is part of the data acquisition pipeline for the project and it is used to get the artists and their metadata.
    It first scrapes the kworb's page to get the artist's names, spotify id, and listeners.
    Then it uses the spotify id to get the artist's metadata from the spotify api.
    The json data is uploaded to a gcs bucket.
"""

GCS_BATCH_SIZE = 250
BASE_URL = "https://kworb.net/spotify/listeners{page_number}.html"


def get_artists_kworb(page_number):
    """Gets the html of the page from kworb's page"""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        # If page number is 1, then the url is the base url for kworb's page.
        if page_number == 1:
            url = BASE_URL.format(page_number="")
        else:
            url = BASE_URL.format(page_number=page_number)

        # return the html of the page
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"Successfully got html of page {page_number} from kworb's page")
        response.encoding = "utf-8"
        return response.text
    except Exception as e:
        logger.error(f"Error getting html page {page_number} for kworb artists: {e}")
        raise RuntimeError(
            f"Error getting html page {page_number} for kworb artists: {e}"
        )


def process_kworb_html(page_number):
    """Processes the html of the page from kworb's page"""
    try:
        html = get_artists_kworb(page_number)

        soup = BeautifulSoup(html, "lxml")
        tr_list = soup.find_all("tr")

        # Get the column map for the artist and listeners columns from the header row to later easily access the data and create a json data structure.
        artist_column_map = {}
        for i, th in enumerate(tr_list[0].find_all("th")):
            # Artist and Listeners are the columns we are interested in from kworb's page
            if th.text.strip() == "Artist" or th.text.strip() == "Listeners":
                artist_column_map[i] = th.text.strip()

        # Create a list of dictionaries for each artist and their data
        artists = []
        for tr in tr_list[1:]:
            # Set these intitial values because we want these to come first in the json data structure.
            individual_artist = {
                "spotify_artist_id": None,
                "artist": None,
                "spotify_url": None,
                "init_processed_at": None,
                "last_processed_at": None,
                "full_blob_name": None,
            }
            for i, td in enumerate(tr.find_all("td")):
                if i in artist_column_map:
                    if td.find("a"):
                        href = td.find("a")["href"]
                        second_part = href.split("/")[-1]
                        spotify_artist_id = second_part.split("_")[0]
                        individual_artist["spotify_artist_id"] = spotify_artist_id
                    if td.text.strip():
                        if artist_column_map[i] == "Listeners":
                            individual_artist["metrics"] = {}
                            individual_artist["metrics"]["kworb"] = {}
                            individual_artist["metrics"]["kworb"][
                                "monthly_listeners"
                            ] = int(td.text.strip().replace(",", ""))
                        elif artist_column_map[i] == "Artist":
                            individual_artist["artist"] = td.text.strip()
                        individual_artist["full_blob_name"] = None
            artists.append(individual_artist)

        logger.info(
            f"Successfully processed artists from kworb's html page {page_number}"
        )
        return artists
    except Exception as e:
        logger.error(
            f"Error processing artists from kworb's html page {page_number}: {e}"
        )
        raise Exception(
            f"Error processing artists from kworb's html page {page_number}: {e}"
        )


def fetch_artists_batch_spotify(batch_artist_list, token, max_retries=3, sleep_time=1):
    """Gets batch of artists from the spotify api"""
    for attempt in range(max_retries):
        try:
            headers = {"Authorization": f"Bearer {token}"}
            url = "https://api.spotify.com/v1/artists"

            spotify_artist_ids = [artist["spotify_artist_id"] for artist in batch_artist_list]
            spotify_artist_ids_str = ",".join(spotify_artist_ids)
            params = {"ids": spotify_artist_ids_str}

            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            backoff_time = sleep_time * (2**attempt)
            logger.warning(
                f"Error getting artists from spotify api: {e}. Retrying in {backoff_time} seconds."
            )
            time.sleep(backoff_time)
    logger.error(
        f"Error getting artists from spotify api. Failed after {max_retries} attempts."
    )
    raise RuntimeError(
        f"Error getting artists from spotify api. Failed after {max_retries} attempts."
    )


def process_spotify_response(artists, batch_size=50):
    """Processes the spotify response for batches of artists"""
    token = get_spotify_access_token()
    try:
        for i in tqdm(range(0, len(artists), batch_size)):
            batch_artist_list = artists[i : i + batch_size]
            response = fetch_artists_batch_spotify(batch_artist_list, token)
            for index, artist in enumerate(batch_artist_list):
                artist["spotify_url"] = response["artists"][index]["external_urls"][
                    "spotify"
                ]
                if artist["init_processed_at"] is None:
                    artist["init_processed_at"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    artist["last_processed_at"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                else:
                    artist["last_processed_at"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

                artist["metrics"]["spotify"] = {}
                artist["metrics"]["spotify"]["followers"] = int(
                    response["artists"][index]["followers"]["total"]
                )
                artist["metrics"]["spotify"]["popularity"] = int(
                    response["artists"][index]["popularity"]
                )
                artist["spotify_meta"] = {}
                artist["spotify_meta"]["genres"] = response["artists"][index]["genres"]
                artist["spotify_meta"]["images"] = response["artists"][index]["images"]
            time.sleep(1)
        return artists
    except Exception as e:
        logger.error(f"Error processing spotify response: {e}")
        raise Exception(f"Error processing spotify response: {e}")


def write_artists_to_gcs(artists, bucket_name, base_blob_name, batch_size=GCS_BATCH_SIZE):
    """Writes the artist list to a json file in a gcp bucket"""
    client = storage.Client.from_service_account_json("gcp_creds.json")
    bucket = client.bucket(bucket_name)
    batch_number = 1
    for i in tqdm(range(0, len(artists), batch_size)):
        try:
            batch_artists = artists[i : i + batch_size]
            for artist in batch_artists:
                artist["full_blob_name"] = f"{base_blob_name}/batch{batch_number}/{artist['spotify_artist_id']}"
            blob = bucket.blob(f"{base_blob_name}/batch{batch_number}/artists.json")
            blob.upload_from_string(
                json.dumps(batch_artists, indent=3, ensure_ascii=False),
                content_type="application/json",
            )
            logger.info(
                f"Successfully wrote artists to gcs bucket {bucket_name} with blob name {base_blob_name}/batch{batch_number}/artists.json"
            )
            batch_number += 1
        except Exception as e:
            logger.error(
                f"Error writing artists to gcs bucket {bucket_name} with blob name {base_blob_name}/batch{batch_number}/artists.json: {e}"
            )
            raise Exception(
                f"Error writing artists to gcs bucket {bucket_name} with blob name {base_blob_name}/batch{batch_number}/artists.json: {e}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--page_number",
        type=int,
        default=1,
        help="The page number of the kworb's page to scrape",
    )
    args = parser.parse_args()
    artists = process_kworb_html(args.page_number)
    artists = process_spotify_response(artists)
    write_artists_to_gcs(
        artists,
        "music-ml-data",
        f"raw-json-data/artists_kworbpage{args.page_number}",
    )
