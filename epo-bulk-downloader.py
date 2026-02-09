from __future__ import annotations

from pathlib import Path
from typing import Any, Optional
import time
import requests

BASE = "https://publication-bdds.apps.epo.org/bdds/bdds-bff-service/prod/api"
PRODUCT_ID = 32
PRODUCT_URL = f"{BASE}/products/{PRODUCT_ID}"
DOWNLOAD_URL = f"{BASE}/products/{PRODUCT_ID}/delivery/{{delivery_id}}/file/{{file_id}}/download"

HEADERS = {"Accept": "application/json", "User-Agent": "bdfs-downloader/1.0"}
CHUNK_SIZE = 1024 * 1024  # 1 MB


# ---------------- HTTP helpers ----------------

def get_json(url: str, retries: int = 6, base_sleep: float = 1.0) -> Any:
    """GET JSON with exponential backoff retries (handles transient 5xx)."""
    last: Optional[Exception] = None
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(base_sleep * (2 ** i))
    raise RuntimeError(f"GET failed: {url}\nLast error: {last}")


def head_content_length(url: str, retries: int = 4, base_sleep: float = 0.6) -> Optional[int]:
    """
    Try to get Content-Length via HEAD. Returns None if unavailable.
    We retry because some servers intermittently fail HEAD.
    """
    last: Optional[Exception] = None
    for i in range(retries):
        try:
            r = requests.head(url, headers=HEADERS, timeout=60, allow_redirects=True)
            r.raise_for_status()
            cl = r.headers.get("Content-Length")
            return int(cl) if cl is not None else None
        except Exception as e:
            last = e
            time.sleep(base_sleep * (2 ** i))
    # If still failing, treat as unknown size
    return None


def download_stream(url: str, out_path: Path, chunk: int = CHUNK_SIZE) -> int:
    """
    Stream download to a .part file then atomically rename to out_path.
    Returns bytes written.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")

    written = 0
    with requests.get(url, headers=HEADERS, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(tmp_path, "wb") as f:
            for b in r.iter_content(chunk_size=chunk):
                if b:
                    f.write(b)
                    written += len(b)

    tmp_path.replace(out_path)
    return written


# ---------------- JSON extraction helpers ----------------

def pick_first(d: dict, keys: list[str]):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def find_delivery_id(delivery: dict):
    return pick_first(delivery, ["id", "deliveryId", "delivery_id", "deliveryID", "uuid", "key"])


def find_files_list(delivery: dict):
    for k in ["files", "file", "items", "content", "assets", "documents"]:
        v = delivery.get(k)
        if isinstance(v, list):
            return v
    return []


def find_file_id(file_obj: dict):
    return pick_first(file_obj, ["id", "fileId", "file_id", "fileID", "uuid", "key"])


def find_file_name(file_obj: dict):
    return pick_first(file_obj, ["name", "fileName", "filename", "originalName"]) or "download.bin"


# ---------------- integrity checks ----------------

def is_fully_downloaded(path: Path, download_url: str) -> bool:
    """
    Consider file complete only if local size matches remote Content-Length from HEAD.
    If Content-Length cannot be determined, return False (force re-download).
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


# ---------------- your processing hook ----------------

def process_archive(path: Path) -> None:
    # TODO: replace with your real logic
    print("Processing:", path.name)


# ---------------- main ----------------

def main():
    product = get_json(PRODUCT_URL)
    deliveries = product.get("deliveries") or product.get("delivery") or []
    print(f"Found {len(deliveries)} deliveries")

    tmp_dir = Path("tmp_bdds")
    tmp_dir.mkdir(exist_ok=True)

    for idx, d in enumerate(deliveries):
        delivery_id = find_delivery_id(d)

        # Sometimes the delivery object is nested
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

            # Sometimes file info is nested too
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

            # Skip only if verified complete by HEAD Content-Length
            if is_fully_downloaded(out_path, url):
                print(f"[skip] already complete: {name}")
                continue

            # If exists but incomplete/unverifiable, remove and re-download
            if out_path.exists():
                print(f"[re-download] incomplete/unverifiable: {name}")
                out_path.unlink(missing_ok=True)

            print(f"Downloading delivery={delivery_id} file={file_id} -> {name}")
            written = download_stream(url, out_path)
            print(f"Downloaded {name} ({written/1e6:.1f} MB)")

            # Optional: verify again after download
            if not is_fully_downloaded(out_path, url):
                raise RuntimeError(f"Downloaded file failed verification: {name}")

            # Optional: process + delete (uncomment if desired)
            # process_archive(out_path)
            # out_path.unlink(missing_ok=True)
            # print("Deleted:", name)


if __name__ == "__main__":
    main()
