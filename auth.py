from dotenv import load_dotenv
import requests
import base64
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

logger = logging.getLogger(__name__)

load_dotenv()

def get_spotify_access_token(num):
    try:
        spotify_client_id = os.getenv(f"SPOTIFY_CLIENT_ID_{num}")
        spotify_client_secret = os.getenv(f"SPOTIFY_CLIENT_SECRET_{num}")

        auth_string = f"{spotify_client_id}:{spotify_client_secret}"
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

        auth_url = "https://accounts.spotify.com/api/token"

        headers = {
            "Authorization": f"Basic {auth_base64}",
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {
            "grant_type": "client_credentials"
        }

        response = requests.post(auth_url, headers=headers, data=data, timeout=10)
        response.raise_for_status()
        return response.json()["access_token"]
    except Exception as e:
        logger.error(f"Error getting Spotify access token: {e}")
        raise