import time
import logging
from playwright.sync_api import sync_playwright
from tqdm import tqdm
from argparse import ArgumentParser
from storage.gcs_utils import get_artists_from_gcs
import requests
import json
from auth import get_spotify_access_token

def get_playlist(playlist_id):
    token = get_spotify_access_token()
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    return response.json()

def process_playlist(playlist_id):
    token = get_spotify_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    song_ids = []

    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    limit = 100
    offset = 0

    while True:
        params = {"limit": limit, "offset": offset}
        response = requests.get(url, headers=headers, params=params)
        data = response.json()

        items = data.get("items", [])
        if not items:
            break

        for item in items:
            track = item.get("track")
            if track and track.get("id"):
                song_ids.append(track["id"])

        offset += len(items)
        if data.get("next") is None:
            break

    return song_ids

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--playlist_id", type=str, required=True)
    args = parser.parse_args()
    song_ids = process_playlist(args.playlist_id)
    print(song_ids)
    print(len(song_ids))