from pathlib import Path
import os
import time
import urllib.request
from urllib.error import URLError, HTTPError

URL = "https://dumps.wikimedia.org/other/pageview_complete/2025/2025-01/pageviews-20250129-automated.bz2"

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = Path(os.environ.get("WIKI_HH_DATA_DIR", REPO_ROOT / "data"))

MAX_RETRIES = 5
SLEEP_SECONDS = 10

OUT_DIR.mkdir(parents=True, exist_ok=True)

filename = URL.split("/")[-1]
out_path = OUT_DIR / filename


def download():
    # Remove partial file if it exists
    if os.path.exists(out_path):
        print(f"Removing partial file: {filename}")
        os.remove(out_path)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Downloading {filename} | attempt {attempt}/{MAX_RETRIES}")

            urllib.request.urlretrieve(URL, out_path)

            size_gb = os.path.getsize(out_path) / 1024**3

            print(f"Downloaded successfully: {filename}")
            print(f"Size: {size_gb:.2f} GB")

            return

        except (URLError, HTTPError, Exception) as e:
            print(f"Failed attempt {attempt}: {e}")

            if os.path.exists(out_path):
                os.remove(out_path)

            if attempt < MAX_RETRIES:
                print(f"Retrying in {SLEEP_SECONDS} seconds...")
                time.sleep(SLEEP_SECONDS)

    print("Download failed completely.")


if __name__ == "__main__":
    download()