from google.cloud import storage
import json
import logging
from tqdm import tqdm
import argparse
import re

from scripts.utils.gcs_utils import get_artists_from_gcs, get_artist_songs_from_gcs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Versions that are truly different recordings/performances
DISTINCT_VERSIONS = [
    "live",
    "acoustic",
    "remix",
    "mix",
    "edit",
    "instrumental",
    "karaoke",
    "demo",
    "outtake",
    "reprise",
    "cover",
    "unplugged",
    "orchestral",
    "a cappella",
    "stripped",
]

# Versions that are the same recording, just repackaged
SAME_RECORDING = [
    "remaster",
    "deluxe",
    "explicit",
    "clean",
    "album version",
    "single version",
    "original version",
    "standard version",
    "film version",
]


def normalize_song_name(title: str) -> str:
    """
    Normalize Spotify track names for grouping.
    'Wonderwall - Remastered 2014' -> 'Wonderwall'
    'Wonderwall - Deluxe' -> 'Wonderwall'
    """
    try:
        if not title:
            return ""

        original_title = title.strip()

        if " - " in original_title:
            before, after = original_title.split(" - ", 1)
            if not any(kw in after.lower() for kw in DISTINCT_VERSIONS):
                title = before
            else:
                title = original_title
        else:
            title = original_title

        patterns = [
            r"\s*\((.*?)?(" + "|".join(SAME_RECORDING) + r").*?\)",
            r"\s*\[(.*?)?(" + "|".join(SAME_RECORDING) + r").*?\]",
            r"\s*\(feat\.?.*?\)",
            r"\s*\[feat\.?.*?\]",
            r"\s*-\s*feat\.?.*$",
        ]

        for pattern in patterns:
            title = re.sub(pattern, "", title, flags=re.IGNORECASE)

        title = re.sub(r"\s+", " ", title).strip()

        return title if title else original_title
    except Exception as e:
        logger.error(f"Error normalizing song name: {e}")
        raise Exception(f"Error normalizing song name: {e}")


def group_songs(artist, bucket_name, songs=None, threshold=1700):
    try:
        if songs is None:
            songs = get_artist_songs_from_gcs(artist, bucket_name)
        
        grouped_songs = {}
        variant_counter = {}
        
        for song in songs:
            normalized_name = normalize_song_name(song["name"])
            
            if normalized_name not in grouped_songs:
                grouped_songs[normalized_name] = {"variants": []}
                variant_counter[normalized_name] = 1
                grouped_songs[normalized_name]["variants"].append(
                    {
                        "artist": song["primary_artist"],
                        "spotify_song_id": song["spotify_song_id"],
                        "name": song["name"],
                        "duration_ms": song["duration_ms"],
                        "album": song["album"],
                    }
                )
            else:
                if (
                    abs(
                        song["duration_ms"]
                        - grouped_songs[normalized_name]["variants"][0]["duration_ms"]
                    )
                ) < threshold:
                    grouped_songs[normalized_name]["variants"].append(
                        {
                            "artist": song["primary_artist"],
                            "spotify_song_id": song["spotify_song_id"],
                            "name": song["name"],
                            "duration_ms": song["duration_ms"],
                            "album": song["album"],
                        }
                    )
                else:
                    variant_counter[normalized_name] += 1
                    new_key = f"{normalized_name}_variant{variant_counter[normalized_name]}"
                    grouped_songs[new_key] = {"variants": []}
                    grouped_songs[new_key]["variants"].append(
                        {
                            "artist": song["primary_artist"],
                            "spotify_song_id": song["spotify_song_id"],
                            "name": song["name"],
                            "duration_ms": song["duration_ms"],
                            "album": song["album"],
                        }
                    )
        return grouped_songs
    except Exception as e:
        logger.error(f"Error grouping song for artist {artist['artist']}: {e}")
        raise Exception(f"Error grouping song for artist {artist['artist']}: {e}")


def write_grouped_songs_to_gcs(artists, bucket_name, base_blob_name):
    try:
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        for artist in tqdm(artists):
            grouped_songs = group_songs(artist, bucket_name)
            blob = bucket.blob(f"{artist['full_blob_name']}/grouped_songs.json")
            blob.upload_from_string(
                json.dumps(grouped_songs, indent=3, ensure_ascii=False)
            )
        logger.info(
            f"Successfully grouped songs for {len(artists)} artists to gcs bucket {bucket_name} with blob name {base_blob_name}"
        )
    except Exception as e:
        logger.error(
            f"Error writing grouped songs to gcs bucket {bucket_name} with blob name {base_blob_name}: {e}"
        )
        raise Exception(
            f"Error writing grouped songs to gcs bucket {bucket_name} with blob name {base_blob_name}: {e}"
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
    write_grouped_songs_to_gcs(
        artists,
        "music-ml-data",
        f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}",
    )