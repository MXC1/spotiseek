from dotenv import load_dotenv
import requests
import time
import uuid
import os

SLSKD_URL = "http://localhost:5030/api/v0"
load_dotenv()
TOKEN = os.getenv("TOKEN")

# List of (artist, track) pairs to search and download
TRACKS = [
    ("MASTER BOOT RECORD", "Skynet")
]

def create_search(search_text):
    search_id = str(uuid.uuid4())
    print(f"[DEBUG] Creating search: id={search_id}, text='{search_text}, TOKEN={TOKEN}'")
    try:
        resp = requests.post(
            f"{SLSKD_URL}/searches",
            json={"id": search_id, "searchText": search_text},
            headers={"X-API-Key": TOKEN}
        )
        print(f"[DEBUG] Search POST status: {resp.status_code}")
        print(f"[DEBUG] Search POST response: {resp.text}")
        resp.raise_for_status()
    except Exception as e:
        print(f"[ERROR] Exception during search creation: {e}")
        raise
    return search_id

def get_search_responses(search_id):
    # Poll for responses
    for i in range(100):
        print(f"[DEBUG] Polling for search responses (attempt {i+1})...")
        try:
            resp = requests.get(
                f"{SLSKD_URL}/searches/{search_id}/responses",
                headers={"X-API-Key": TOKEN}
            )
            print(f"[DEBUG] Responses GET status: {resp.status_code}")
            print(f"[DEBUG] Responses GET response: {resp.text}")
            resp.raise_for_status()
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                print(f"[DEBUG] Found {len(data)} responses.")
                return data
        except Exception as e:
            print(f"[ERROR] Exception during response polling: {e}")
        time.sleep(2)
    print(f"[DEBUG] No responses found after polling.")
    return []

def enqueue_download(search_id, fileinfo, username):
    print(f"[DEBUG] Enqueuing download for search_id={search_id}, username={username}, fileinfo={fileinfo}")
    try:
        url = f"{SLSKD_URL}/transfers/downloads/{username}"
        payload = [{**fileinfo, "username": username}]
        resp = requests.post(
            url,
            json=payload,
            headers={"X-API-Key": TOKEN}
        )
        print(f"[DEBUG] Download POST status: {resp.status_code}")
        print(f"[DEBUG] Download POST response: {resp.text}")
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[ERROR] Exception during download enqueue: {e}")
        raise

def main():
    for artist, track in TRACKS:
        search_text = f"{artist} {track}"
        print(f"Searching for: {search_text}")
        try:
            search_id = create_search(search_text)
            responses = get_search_responses(search_id)
            if not responses:
                print(f"No results for {artist} {track}")
                continue
            # Each response contains 'username', 'files', etc.
            # Find the first file in the first response
            first_response = responses[0]
            files = first_response.get("files", [])
            username = first_response.get("username")
            if not files:
                print(f"No files found for {artist} {track}")
                continue
            first_file = files[0]
            filename = first_file.get("filename")
            size = first_file.get("size")
            fileinfo = {"filename": filename, "size": size}
            print(f"Downloading: {filename} (size: {size}) from {username}")
            download_resp = enqueue_download(search_id, fileinfo, username)
            print(f"Download started: {download_resp}")
        except Exception as e:
            print(f"[ERROR] Exception in main loop for {artist} {track}: {e}")
        time.sleep(2)

if __name__ == "__main__":
    main()
