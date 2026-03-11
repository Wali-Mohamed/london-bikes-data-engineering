import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
import os
import time

BASE_URL = "https://cycling.data.tfl.gov.uk/"
S3_API = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk?list-type=2"

DOWNLOAD_FOLDER = "data/raw"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)


def get_file_list():
    """Discover CSV files from TfL S3 bucket"""

    response = requests.get(S3_API)
    root = ET.fromstring(response.text)

    # remove XML namespace
    for elem in root.iter():
        elem.tag = elem.tag.split("}")[-1]

    files = []

    for item in root.findall(".//Contents"):
        key = item.find("Key").text

        if key.endswith(".csv") and "JourneyDataExtract" in key:
            files.append(BASE_URL + key)

    return files


def download_file(url):
    """Download a single file"""

    filename = url.split("/")[-1]
    filepath = os.path.join(DOWNLOAD_FOLDER, filename)

    if os.path.exists(filepath):
        print("Skipping:", filename)
        return

    print("Downloading:", filename)

    r = requests.get(url, stream=True)

    with open(filepath, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)


def main():

    start_time = time.time()

    files = get_file_list()

    print(f"Found {len(files)} CSV files")

    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(download_file, files)

    end_time = time.time()

    print("\nDownload completed")
    print(f"Total time: {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()