import logging
from logs_utils import setup_logging
from dotenv import load_dotenv
import requests
import time
import uuid
import os
from database.database_management import TrackDB

setup_logging(log_name_prefix="slskd_downloader")
load_dotenv()

SLSKD_URL = "http://localhost:5030/api/v0"
TOKEN = os.getenv("TOKEN")

# Initialize the database
track_db = TrackDB()

def create_search(search_text):
    """Create a search on the Soulseek server."""
    search_id = str(uuid.uuid4())
    logging.debug(f"Creating search: id={search_id}, text='{search_text}', TOKEN={TOKEN}")
    try:
        resp = requests.post(
            f"{SLSKD_URL}/searches",
            json={"id": search_id, "searchText": search_text},
            headers={"X-API-Key": TOKEN}
        )
        logging.debug(f"Search POST status: {resp.status_code}")
        logging.debug(f"Search POST response: {resp.text}")
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Exception during search creation: {e}")
        raise
    return search_id

def get_search_responses(search_id):
    """Poll for search responses from the Soulseek server."""
    for i in range(100):
        logging.debug(f"Polling for search responses (attempt {i+1})...")
        try:
            resp = requests.get(
                f"{SLSKD_URL}/searches/{search_id}/responses",
                headers={"X-API-Key": TOKEN}
            )
            logging.debug(f"Responses GET status: {resp.status_code}")
            logging.debug(f"Responses GET response: {resp.text}")
            resp.raise_for_status()
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                logging.debug(f"Found {len(data)} responses.")
                return data
        except Exception as e:
            logging.error(f"Exception during response polling: {e}")
        time.sleep(2)
    logging.debug(f"No responses found after polling.")
    return []

def enqueue_download(search_id, fileinfo, username):
    """Enqueue a file for download on the Soulseek server."""
    logging.debug(f"Enqueuing download for search_id={search_id}, username={username}, fileinfo={fileinfo}")
    try:
        url = f"{SLSKD_URL}/transfers/downloads/{username}"
        payload = [{**fileinfo, "username": username}]
        resp = requests.post(
            url,
            json=payload,
            headers={"X-API-Key": TOKEN}
        )
        logging.debug(f"Download POST status: {resp.status_code}")
        logging.debug(f"Download POST response: {resp.text}")
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error(f"Exception during download enqueue: {e}")
        raise

def download_track(artist, track, spotify_id):
    """Search for and download a specific track."""
    search_text = f"{artist} {track}"
    logging.info(f"Searching for: {search_text}")
    track_db.update_track_status(spotify_id, "searching")
    try:
        search_id = create_search(search_text)
        responses = get_search_responses(search_id)
        if not responses:
            logging.info(f"No results for {artist} {track}")
            track_db.update_track_status(spotify_id, "Failed")
            return
        # Each response contains 'username', 'files', etc.
        # Find the first file in the first response
        first_response = responses[0]
        files = first_response.get("files", [])
        username = first_response.get("username")
        if not files:
            logging.info(f"No files found for {artist} {track}")
            track_db.update_track_status(spotify_id, "failed")
            return
        first_file = files[0]
        filename = first_file.get("filename")
        size = first_file.get("size")
        fileinfo = {"filename": filename, "size": size}
        logging.info(f"Downloading: {filename}")
        download_resp = enqueue_download(search_id, fileinfo, username)
        logging.debug(f"Download started: {download_resp}")
        track_db.update_track_status(spotify_id, "downloading", file_path=filename)
    except Exception as e:
        logging.error(f"Exception while downloading track '{artist} {track}': {e}")
        track_db.update_track_status(spotify_id, "failed")

if __name__ == "__main__":
    # Example usage
    for id, artist, track in [
        ("5ms8IkagrFWObtzSOahVrx", "MASTER BOOT RECORD", "Skynet")
    ]:
        track_id = track_db.add_track(spotify_id=id, track_name=track, artist=artist)
        download_track(artist, track, track_id)

    # Close the database connection
    track_db.close()
