import pandas as pd
import argparse

from scripts.utils.gcs_utils import get_artists_from_gcs

BUCKET_NAME = "music-ml-data"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--page_number", type=int, default=1, help="The page number of the kworb's page to scrape")
    parser.add_argument("--batch_number", type=int, default=1, help="The batch number of the artists")
    args = parser.parse_args()

    artists = get_artists_from_gcs(
        BUCKET_NAME,
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
    )