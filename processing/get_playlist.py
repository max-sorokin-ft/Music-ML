import time
import logging
from playwright.sync_api import sync_playwright
from tqdm import tqdm
from argparse import ArgumentParser
from storage.gcs_utils import get_artists_from_gcs
from google.cloud import storage
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
    playlist = get_playlist(playlist_id)

    song_ids = []  # list to store track IDs

    for track in playlist["tracks"]["items"]:
        song_id = track["track"]["id"]
        song_ids.append(song_id)

    return song_ids