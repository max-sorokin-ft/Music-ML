import time
import logging
from playwright.sync_api import sync_playwright
from tqdm import tqdm
from argparse import ArgumentParser
from scripts.utils.gcs_utils import get_artists_from_gcs
from google.cloud import storage
import json

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


BASE_URL = "https://www.chosic.com/music-genre-finder/"
SPOTIFY_URL = "https://open.spotify.com/artist/{artist_id}"


def get_artist_genres(artist_id):
    """Simple genre scraper using Playwright"""
    try:
        artist_url = SPOTIFY_URL.format(artist_id=artist_id)

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                ],
            )

            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            )

            page = context.new_page()

            page.goto(BASE_URL)

            page.wait_for_selector("#suggestion-options", timeout=10000)

            page.select_option("#suggestion-options", value="artistUrl")
            page.fill("#search-word", artist_url)
            page.click(".btn-search")
            page.wait_for_selector("#spotify-tags a", timeout=20000)

            time.sleep(3)

            spotify_genre_elements = page.query_selector_all("#spotify-tags a")
            spotify_genres = [elem.inner_text() for elem in spotify_genre_elements]

            browser.close()

            return spotify_genres
    except Exception as e:
        logger.error(f"Error getting genres for artist {artist_id}: {e}")
        raise Exception(f"Error getting genres for artist {artist_id}: {e}")


def write_genres_to_gcs(artists, bucket_name, base_blob_name):
    """Writes the genres to the gcs bucket"""
    try:
        updated_artists = []
        for artist in tqdm(artists[:30]):
            genres = get_artist_genres(artist["spotify_artist_id"])
            artist["genres"] = genres
            logger.info(
                f"Successfully got genres {genres} for artist {artist['artist']}"
            )
            updated_artists.append(artist)

        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"{base_blob_name}/artists.json")
        blob.upload_from_string(
            json.dumps(updated_artists, indent=3, ensure_ascii=False),
            content_type="application/json",
        )
        logger.info(
            f"Successfully wrote genres for {len(updated_artists)} artists to gcs bucket {bucket_name} with blob name {base_blob_name}/artists.json"
        )
    except Exception as e:
        logger.error(
            f"Error writing genres to gcs bucket {bucket_name} with blob name {base_blob_name}/artists.json: {e}"
        )
        raise Exception(
            f"Error writing genres to gcs bucket {bucket_name} with blob name {base_blob_name}/artists.json: {e}"
        )


if __name__ == "__main__":
    parser = ArgumentParser()
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
    write_genres_to_gcs(
        artists,
        "music-ml-data",
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}",
    )
