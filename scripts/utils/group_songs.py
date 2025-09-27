from locale import normalize
from google.cloud import storage
import requests
import json
import logging
import time
from tqdm import tqdm
import argparse
import re

from scripts.utils.gcs_utils import get_artists_from_gcs
from scripts.raw_data.get_songs import get_all_artist_songs_from_gcs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

import re

KEEP_KEYWORDS = [
    "live", "acoustic", "remix", "mix", "edit",
    "instrumental", "karaoke", "version", "outtake", "reprise"
]

SAFE_REMOVE = [
    "remaster", "deluxe", "explicit", "clean", "radio edit",
    "album version", "single version", "extended mix", "film version"
]

def normalize_song_name(title: str) -> str:
    """
    Normalize Spotify track names for grouping.
    """
    if not title:
        return ""

    original_title = title.strip()

    # Step 1: Handle dash suffix (only " - " with spaces)
    if " - " in original_title:
        before, after = original_title.split(" - ", 1)
        if not any(kw in after.lower() for kw in KEEP_KEYWORDS):
            title = before
        else:
            title = original_title
    else:
        title = original_title

    # Step 2: Remove safe patterns in () and []
    patterns = [
        r'\s*\((.*?)?(' + "|".join(SAFE_REMOVE) + r').*?\)',
        r'\s*\[(.*?)?(' + "|".join(SAFE_REMOVE) + r').*?\]',
        r'\s*\(feat\.?.*?\)',
        r'\s*\[feat\.?.*?\]',
        r'\s*-\s*feat\.?.*$'
    ]

    for pattern in patterns:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)

    # Step 3: Cleanup
    title = re.sub(r'\s+', ' ', title).strip()

    return title if title else original_title

def group_songs(artists, bucket_name, threshold=1700):
    for artist in artists[:1]:
        songs = get_all_artist_songs_from_gcs(artist, bucket_name)
        grouped_songs = {}
        for song in songs:
            normalized_name = normalize_song_name(song["name"])
            if normalized_name not in grouped_songs:
                grouped_songs[normalized_name] = {
                    "variants": []
                }
                grouped_songs[normalized_name]["variants"].append({
                    "id": song["spotify_song_id"],
                    "name": song["name"],
                    "duration_ms": song["duration_ms"],
                    "album": song["album"],
                })
            else:
                if (abs(song["duration_ms"] - grouped_songs[normalized_name]["variants"][0]["duration_ms"])) < threshold:
                    grouped_songs[normalized_name]["variants"].append({
                        "id": song["spotify_song_id"],
                        "name": song["name"],
                        "duration_ms": song["duration_ms"],
                        "album": song["album"],
                    })
                elif (abs(song["duration_ms"] - grouped_songs[normalized_name]["variants"][0]["duration_ms"])) > threshold:
                    grouped_songs[normalized_name] = {
                        "variants": []
                    }
                    grouped_songs[normalized_name]["variants"].append({
                        "id": song["spotify_song_id"],
                        "name": song["name"],
                        "duration_ms": song["duration_ms"],
                        "album": song["album"],
                    })
    return grouped_songs




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
    songs = group_songs(artists, "music-ml-data")
    with open("grouped_songs.json", "w") as f:
        json.dump(songs, f, indent=3, ensure_ascii=False)

