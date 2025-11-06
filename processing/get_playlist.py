import argparse
import logging
import requests
import pandas as pd
import gcsfs
from io import BytesIO
from google.cloud import storage
from auth import get_spotify_access_token

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BUCKET_NAME = "music-ml-data"
fs = gcsfs.GCSFileSystem(token="gcp_creds.json")

def gcs_read_parquet(path):
    return pd.read_parquet(path, filesystem=fs)

def gcs_write_parquet(df, path):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    client = storage.Client.from_service_account_json("gcp_creds.json")
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(path.replace(f"gs://{BUCKET_NAME}/", ""))
    blob.upload_from_file(buffer, content_type="application/parquet")

def get_spotify_playlist_songs(playlist_id):
    token = get_spotify_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    song_ids = []
    offset = 0
    while True:
        r = requests.get(url, headers=headers, params={"limit": 100, "offset": offset})
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        if not items:
            break
        for it in items:
            t = it.get("track")
            if t and t.get("id"):
                song_ids.append(t["id"])
        offset += len(items)
        if data.get("next") is None:
            break
    return set(song_ids)

def deezer_id_from_isrc(isrc):
    if not isrc:
        return None
    try:
        a = requests.get(f"https://api.deezer.com/track/isrc:{isrc}", timeout=15).json()
        if a.get("id"):
            return str(int(a["id"]))
    except:
        pass
    try:
        b = requests.get(f"https://api.deezer.com/search?q=isrc:{isrc}", timeout=15).json()
        if b.get("data"):
            return str(int(b["data"][0]["id"]))
    except:
        pass
    return None

def ms_to_mmss(ms):
    s = int(ms // 1000)
    return f"{s//60}:{s%60:02d}"

def process_playlist(page, batch, playlist_id):
    base = f"gs://{BUCKET_NAME}/parquet_metadata/artists_kworbpage{page}/batch{batch}"
    df_all = gcs_read_parquet(f"{base}/songs.parquet")

    playlist_ids = get_spotify_playlist_songs(playlist_id)
    df = df_all[df_all["spotify_song_id"].isin(playlist_ids)].copy()

    df_filtered = df_all[df_all["duration_ms"] >= 120000]
    artist_groups = df_filtered.groupby("spotify_artist_id")["duration_ms"].mean()
    artist_avg_ms = artist_groups.to_dict()

    df["avg_artist_length_ms"] = df["spotify_artist_id"].map(artist_avg_ms)
    df["avg_artist_length"] = df["avg_artist_length_ms"].apply(lambda ms: ms_to_mmss(ms) if ms else None)
    df["deezer_id"] = df["isrc"].apply(deezer_id_from_isrc)

    CUT_420 = 260000
    CUT_500 = 300000
    AVG_435 = 275000

    df["use_deezer"] = False
    df["use_spotify"] = False

    df.loc[df["duration_ms"] < CUT_420, "use_deezer"] = True
    df.loc[df["duration_ms"] >= CUT_500, "use_spotify"] = True

    mid = (df["duration_ms"] >= CUT_420) & (df["duration_ms"] < CUT_500)

    df.loc[mid & (df["avg_artist_length_ms"] <= AVG_435), ["use_deezer", "use_spotify"]] = True
    df.loc[mid & (df["avg_artist_length_ms"] > AVG_435), "use_spotify"] = True

    gcs_write_parquet(df, f"{base}/playlist.parquet")
    logger.info(f"Wrote {base}/playlist.parquet")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--page_number", type=int, default=1)
    p.add_argument("--batch_number", type=int, default=1)
    p.add_argument("--playlist_id", type=str, required=True)
    a = p.parse_args()
    process_playlist(a.page_number, a.batch_number, a.playlist_id)