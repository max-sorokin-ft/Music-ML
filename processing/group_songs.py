from google.cloud import storage
import json
import logging
from tqdm import tqdm
import argparse
import re
import unicodedata
import string

from storage.gcs_utils import get_artists_from_gcs, get_artist_songs_from_gcs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

BUCKET_NAME = "music-ml-data"

# Versions that are truly different recordings/performances
DISTINCT_VERSIONS = {
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
    "cappella",
    "acapella",
    "stripped",
    "strings",
    "session",
    "rehearsal",
    "bootleg",
    "alternate",
}

# Versions that are the same recording, just repackaged
SAME_RECORDING = {
    "remastered",
    "remaster",
    "remastering",
    "deluxe",
    "explicit",
    "clean",
    "album",
    "version",
    "single",
    "original",
    "standard",
    "film",
    "edition",
    "anniversary",
    "expanded",
    "extended",
    "bonus",
    "special",
    "collectors",
    "collector",
    "limited",
    "radio",
    "digital",
    "vinyl",
    "cd",
    "stereo",
    "mono",
}


def normalize_song_name(title: str) -> str:
    """
    Normalize Spotify track names for grouping.
    
    Examples:
        'Supersonic - Remastered' -> 'supersonic'
        'Wonderwall (Live at Wembley)' -> 'wonderwall live'
        'Champagne Supernova - Deluxe Edition' -> 'champagne supernova'
    """
    if not title:
        return ""
    
    original_title = title
    
    title = unicodedata.normalize("NFKD", title).casefold()
    
    feat_patterns = [
        r'\s*[\(\[\{]\s*feat\.?[^\)\]\}]*[\)\]\}]',  # (feat. X)
        r'\s*[\(\[\{]\s*ft\.?[^\)\]\}]*[\)\]\}]',     # (ft. X)
        r'\s*[\(\[\{]\s*featuring[^\)\]\}]*[\)\]\}]', # (featuring X)
        r'\s*-\s*feat\.?.*$',                          # - feat. X
        r'\s*-\s*ft\.?.*$',                            # - ft. X
        r'\s*-\s*featuring.*$',                        # - featuring X
    ]
    
    for pattern in feat_patterns:
        title = re.sub(pattern, '', title, flags=re.IGNORECASE)

    title = re.sub(r'\s*[\(\[\{]\s*\d{4}\s*[\)\]\}]', '', title)
    # Remove standalone 4-digit years starting with 10..20 (e.g., 1995, 2011) anywhere in the title
    title = re.sub(r'\b(1[0-9]{3}|20[0-9]{2})\b', '', title)
    
    title = title.translate(str.maketrans('', '', string.punctuation))
    
    title = re.sub(r'\s+', ' ', title).strip()
    
    words = title.split()
    
    if not words:
        return original_title.lower()
    
    filtered_words = []
    for word in words:
        if word in DISTINCT_VERSIONS:
            filtered_words.append(word)
        elif word not in SAME_RECORDING:
            filtered_words.append(word)
    
    result = ' '.join(filtered_words).strip()
    
    return result if result else original_title.lower()


def group_songs(artist, bucket_name, songs=None, threshold=20000):
    try: 
        grouped_songs = {}
        variant_counter = {}
        
        # There are cases in the repo where songs can be passed in from memory/other functions, if not we get them from GCS
        if songs is None:
            songs = get_artist_songs_from_gcs(artist, bucket_name)

        for song in songs:
            normalized_name = normalize_song_name(song["song"])

            if normalized_name not in grouped_songs:
                grouped_songs[normalized_name] = {"variants": []}
                variant_counter[normalized_name] = 1
                grouped_songs[normalized_name]["variants"].append(
                    {
                        "artists": song["artists"],
                        "artist_ids": song["artist_ids"],
                        "spotify_song_id": song["spotify_song_id"],
                        "song": song["song"],
                        "duration_ms": song["duration_ms"],
                        "album": song["album"],
                        "spotify_popularity": song["spotify_popularity"],
                        "isrc": song["isrc"],
                    }
                )
            else:
                matched_key = normalized_name
                found_match = False
                for i in range(1, variant_counter[normalized_name] + 1):
                    matched_key = normalized_name if i == 1 else f"{normalized_name}_variant{i}"

                    for grouped_song in grouped_songs[matched_key]["variants"]:
                        duration_diff = abs(
                            song["duration_ms"]
                            - grouped_song["duration_ms"]
                        )
                        if duration_diff < threshold:
                            found_match = True
                            break
                    
                    if not found_match:
                        break
                    
                if found_match:
                    grouped_songs[matched_key]["variants"].append(
                        {
                            "artists": song["artists"],
                            "artist_ids": song["artist_ids"],
                            "spotify_song_id": song["spotify_song_id"],
                            "song": song["song"],
                            "duration_ms": song["duration_ms"],
                            "album": song["album"],
                            "spotify_popularity": song["spotify_popularity"],
                            "isrc": song["isrc"],
                        }
                    )
                else:
                    variant_counter[normalized_name] += 1
                    new_key = f"{normalized_name}_variant{variant_counter[normalized_name]}"
                    grouped_songs[new_key] = {"variants": []}
                    grouped_songs[new_key]["variants"].append(
                        {
                            "artists": song["artists"],
                            "artist_ids": song["artist_ids"],
                            "spotify_song_id": song["spotify_song_id"],
                            "song": song["song"],
                            "duration_ms": song["duration_ms"],
                            "album": song["album"],
                            "spotify_popularity": song["spotify_popularity"],
                            "isrc": song["isrc"],
                        }
                    )
        
        return grouped_songs
    except Exception as e:
        logger.error(f"Error grouping song for artist {artist['artist']}: {e}")
        raise


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
        raise


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

    try:
        artists = get_artists_from_gcs(
            BUCKET_NAME,
            f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}/artists.json",
        )
        write_grouped_songs_to_gcs(
            artists,
            BUCKET_NAME,
            f"raw-json-data/artists_kworbpage{args.page_number}/batch{args.batch_number}",
        )
    except Exception as e:
        logger.error(f"Error running the script group_songs.py: {e}")
        raise