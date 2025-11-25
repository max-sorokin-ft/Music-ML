import time
import random
import logging
from playwright.sync_api import sync_playwright
from tqdm import tqdm
from argparse import ArgumentParser
from storage.gcs_utils import get_artists_from_gcs
from google.cloud import storage
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.chosic.com/music-genre-finder/"
SPOTIFY_URL = "https://open.spotify.com/artist/{artist_id}"
BUCKET_NAME = "music--data"


def create_browser():
    p = sync_playwright().start()
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ],
    )
    return p, browser


def get_artist_genres(browser, artist_id):
    try:
        artist_url = SPOTIFY_URL.format(artist_id=artist_id)

        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
        )

        page = context.new_page()
        page.goto(BASE_URL)
        page.wait_for_selector("#suggestion-options", timeout=10000)

        page.mouse.move(
            random.randint(400, 1400),
            random.randint(300, 700),
            steps=random.randint(12, 20)
        )

        page.evaluate("window.scrollTo(0, document.body.scrollHeight * Math.random() * 0.5)")
        time.sleep(random.uniform(2.0, 4.0))

        page.select_option("#suggestion-options", value="artistUrl")
        page.fill("#search-word", artist_url)
        page.click(".btn-search")

        page.wait_for_selector("#spotify-tags a", timeout=20000)
        time.sleep(random.uniform(2.5, 5.0))

        spotify_genre_elements = page.query_selector_all("#spotify-tags a")
        spotify_genres = [elem.inner_text() for elem in spotify_genre_elements]

        context.close()
        return spotify_genres

    except Exception as e:
        logger.error(f"Error getting genres for artist {artist_id}: {e}")
        raise


def write_genres_to_gcs(artists, bucket_name, base_blob_name):
    try:
        updated_artists = []

        p, browser = create_browser()

        for artist in tqdm(artists):
            try:
                genres = get_artist_genres(browser, artist["spotify_artist_id"])
                artist["genres"] = genres
                logger.info(f"Got genres {genres} for artist {artist['artist']}")
            except Exception as e:
                logger.error(f"Failed to get genres for {artist['artist']}: {e}")
                artist["genres"] = []
            updated_artists.append(artist)

        browser.close()
        p.stop()

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{base_blob_name}/artists.json")

        blob.upload_from_string(
            json.dumps(updated_artists, indent=3, ensure_ascii=False),
            content_type="application/json",
        )

        logger.info(f"Wrote {len(updated_artists)} artists to gs://{bucket_name}/{base_blob_name}/artists.json")

    except Exception as e:
        logger.error(f"Error writing genres to GCS: {e}")
        raise


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--page_number", type=int, default=1)
    parser.add_argument("--batch_number", type=int, default=1)
    args = parser.parse_args()

    try:
        artists = get_artists_from_gcs(
            BUCKET_NAME,
            f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
        )

        write_genres_to_gcs(
            artists,
            BUCKET_NAME,
            f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}",
        )

    except Exception as e:
        logger.error(f"Error running get_genres.py: {e}")
        raise
