from __future__ import annotations

from pathlib import Path
from typing import Any, Optional
import time
import requests

# Base URL for EPO Bulk Data Downloader API
BASE_URL = "https://publication-bdds.apps.epo.org/bdds/bdds-bff-service/prod/api"
# Example product ID for ("14.12 EP full-text data"). Replace with your desired product ID. See ep_bulk_download_codes.json for available products.
PRODUCT_ID = 32
# URL to fetch product metadata, which includes deliveries and files info.
PRODUCT_URL = f"{BASE_URL}/products/{PRODUCT_ID}" 
# URL template for downloading files. delivery_id and file_id will be filled in from the product metadata.
DOWNLOAD_URL = f"{BASE_URL}/products/{PRODUCT_ID}/delivery/{{delivery_id}}/file/{{file_id}}/download"
# You can customize the User-Agent if needed.
HEADERS = {"Accept": "application/json", "User-Agent": "bdfs-downloader/1.0"}
# 1 MB chunks for streaming download
CHUNK_SIZE = 1024 * 1024


#---------------- helper functions ----------------

def get_json(url: str, retries: int = 6, base_sleep: float = 1.0) -> Any:
    """
    We retry with exponential backoff because some servers intermittently fail.
    
    :param url: URL to fetch JSON from
    :type url: str
    :param retries: Number of retries before giving up
    :type retries: int
    :param base_sleep: Base sleep time in seconds for exponential backoff (total wait grows as base_sleep * (2^i))
    :type base_sleep: float
    :return: Parsed JSON response
    :rtype: Any
    """
    last: Optional[Exception] = None
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60) # You can adjust timeout as needed
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(base_sleep * (2 ** i))
    raise RuntimeError(f"GET failed: {url}\nLast error: {last}")


def head_content_length(url: str, retries: int = 4, base_sleep: float = 0.6) -> Optional[int]:
    """
    Perform a HEAD request to get Content-Length. Retry on failure. Return None if Content-Length is missing or on repeated failures.
    
    :param url: URL to check
    :type url: str
    :param retries: Number of retries before giving up
    :type retries: int
    :param base_sleep: Base sleep time in seconds for exponential backoff (total wait grows as base_sleep * (2^i))
    :type base_sleep: float
    :return: Content-Length in bytes, or None if not available or on failure
    :rtype: Optional[int]
    """
    last: Optional[Exception] = None
    for i in range(retries):
        try:
            r = requests.head(url, headers=HEADERS, timeout=60, allow_redirects=True) # HEAD request to get headers (follow redirects if any)
            r.raise_for_status()
            cl = r.headers.get("Content-Length")
            return int(cl) if cl is not None else None # Return Content-Length as int, or None if missing
        except Exception as e:
            last = e
            time.sleep(base_sleep * (2 ** i))
    print(f"[warn] HEAD failed for {url}\nLast error: {last}")
    return None


def download_stream(url: str, out_path: Path, chunk: int = CHUNK_SIZE) -> int:
    """
    Download a file from the given URL to the specified output path using streaming. 
    Writes to a temporary file first and then renames it to ensure atomicity. 
    Returns the total number of bytes written.
    
    :param url: URL to download from
    :type url: str
    :param out_path: Path to save the downloaded file
    :type out_path: Path
    :param chunk: Chunk size in bytes for streaming download
    :type chunk: int
    :return: Total bytes written to the file
    :rtype: int
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")

    written = 0
    with requests.get(url, headers=HEADERS, stream=True, timeout=300) as r: # Stream the download to handle large files without loading into memory
        r.raise_for_status()
        with open(tmp_path, "wb") as f:
            for b in r.iter_content(chunk_size=chunk):
                if b:
                    f.write(b)
                    written += len(b)

    tmp_path.replace(out_path)
    return written


#---------------- utility functions for flexible metadata extraction ----------------

def pick_first(d: dict, keys: list[str]):
    """
    Try multiple possible keys in the given dictionary and return the value for the first key that exists and is not None. 
    If none of the keys are found or all values are None, return None.
    
    :param d: Description
    :type d: dict
    :param keys: Description
    :type keys: list[str]
    """
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def find_delivery_id(delivery: dict):
    """
    Try multiple possible keys in the delivery metadata to find the delivery ID, which is needed for constructing the download URL. 
    The actual key may vary across products or deliveries, so we check several common possibilities. 
    If none are found, we return None to indicate we can't identify the delivery for downloading.
    
    :param delivery: Description
    :type delivery: dict
    """
    return pick_first(delivery, ["id", "deliveryId", "delivery_id", "deliveryID", "uuid", "key"])

def find_files_list(delivery: dict):
    """
    Try multiple possible keys in the delivery metadata to find the list of files to download.
    The actual key may vary across products or deliveries, so we check several common possibilities.
    If none are found, we return an empty list to indicate no files to process for this delivery.
    
    :param delivery: Description
    :type delivery: dict
    """
    for k in ["files", "file", "items", "content", "assets", "documents"]: # Common keys that might contain the list of files in the delivery metadata.
        v = delivery.get(k)
        if isinstance(v, list):
            return v
    return []


def find_file_id(file_obj: dict):
    """
    Try multiple possible keys in the file metadata to find the file ID, which is needed for constructing the download URL.
    The actual key may vary across products or files, so we check several common possibilities.
    If none are found, we return None to indicate we can't identify the file for downloading.
    
    :param file_obj: Description
    :type file_obj: dict
    """
    return pick_first(file_obj, ["id", "fileId", "file_id", "fileID", "uuid", "key"]) # Common keys that might contain the file ID in the file metadata.


def find_file_name(file_obj: dict):
    """
    Try multiple possible keys in the file metadata to find the filename.
    The actual key may vary across products or files, so we check several common possibilities.
    If none are found, we return a default name "download.bin" to avoid errors, but you may want to handle this case differently depending on your needs.
    
    :param file_obj: Description
    :type file_obj: dict
    """
    return pick_first(file_obj, ["name", "fileName", "filename", "originalName"]) or "download.bin" # Common keys that might contain the filename in the file metadata.


# ---------------- download verification ----------------

def is_fully_downloaded(path: Path, download_url: str) -> bool:
    """
    Check if the local file exists and its size matches the remote Content-Length from a HEAD request. 
    This helps avoid re-downloading files that are already complete, while still allowing us to detect 
    incomplete or corrupted downloads and re-download them as needed.
    
    :param path: Path to the local file to check
    :type path: Path
    :param download_url: URL to check the remote file size via HEAD request
    :type download_url: str
    :return: True if the local file exists and its size matches the remote Content-Length, False otherwise
    :rtype: bool
    """
    if not path.exists() or not path.is_file():
        return False

    local_size = path.stat().st_size
    if local_size <= 0:
        return False

    remote_size = head_content_length(download_url)
    if remote_size is None:
        print(f"[warn] no Content-Length for {path.name}; cannot verify -> will re-download") 
        return False

    if local_size != remote_size:
        print(f"[warn] size mismatch for {path.name}: local={local_size} remote={remote_size}")
        return False

    return True


# TODO: implement your processing logic for the downloaded archives.

def process_archive(path: Path) -> None:
    # TODO: replace with your real logic
    print("Processing:", path.name)


# ---------------- main ----------------

def main():
    """
    Main function to orchestrate the downloading of files from the EPO Bulk Data Downloader API.
    It fetches the product metadata, iterates through deliveries and files, 
    checks if each file is already fully downloaded, and if not, 
    downloads it while handling potential issues with retries and verification.    
    """
    product = get_json(PRODUCT_URL) # Fetch the product metadata.
    deliveries = product.get("deliveries") or product.get("delivery") or [] # The deliveries information may be under different keys depending on the product structure, so we check several common possibilities.
    print(f"Found {len(deliveries)} deliveries") 

    tmp_dir = Path("tmp_bdds")
    tmp_dir.mkdir(exist_ok=True)

    for idx, d in enumerate(deliveries):
        delivery_id = find_delivery_id(d)
        
        # Sometimes the delivery metadata might have nested structures.
        if delivery_id is None: 
            for nested_key in ["delivery", "meta", "data"]:
                if isinstance(d.get(nested_key), dict):
                    delivery_id = find_delivery_id(d[nested_key])
                    if delivery_id is not None:
                        d = d[nested_key]
                        break

        if delivery_id is None:
            print(f"[skip] delivery #{idx}: couldn't find delivery id. keys={list(d.keys())}")
            continue

        files = find_files_list(d)
        if not files:
            print(f"[skip] delivery {delivery_id}: no files list in keys={list(d.keys())}")
            continue

        for fobj in files:
            file_id = find_file_id(fobj)

            # Sometimes the file metadata might have nested structures.
            if file_id is None:
                for nested_key in ["file", "data", "meta"]:
                    if isinstance(fobj.get(nested_key), dict):
                        file_id = find_file_id(fobj[nested_key])
                        if file_id is not None:
                            fobj = fobj[nested_key]
                            break

            if file_id is None:
                print(f"[skip] delivery {delivery_id}: couldn't find file id. keys={list(fobj.keys())}")
                continue

            name = find_file_name(fobj)
            out_path = tmp_dir / name

            url = DOWNLOAD_URL.format(delivery_id=delivery_id, file_id=file_id)

            # First, we check if the file is already fully downloaded.
            if is_fully_downloaded(out_path, url):
                print(f"[skip] already complete: {name}")
                continue

            # If we reach this point, it means the file is either not downloaded or incomplete/corrupted, so we proceed to download it.
            if out_path.exists():
                print(f"[re-download] incomplete/unverifiable: {name}")
                out_path.unlink(missing_ok=True)

            print(f"Downloading delivery={delivery_id} file={file_id} -> {name}") 
            written = download_stream(url, out_path)
            print(f"Downloaded {name} ({written/1e6:.1f} MB)")

            if not is_fully_downloaded(out_path, url):
                raise RuntimeError(f"Downloaded file failed verification: {name}")

            # Optional: process + delete (uncomment if desired)
            # process_archive(out_path)
            # out_path.unlink(missing_ok=True)
            # print("Deleted:", name)


if __name__ == "__main__":
    main()
