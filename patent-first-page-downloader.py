from dotenv import load_dotenv
import os
import csv
import time
import random
import threading
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm

# ---------------- CONFIG ----------------
load_dotenv()

OPS_KEY = os.environ["EPO_OPS_KEY"]
OPS_SECRET = os.environ["EPO_OPS_SECRET"]

BASE = "https://ops.epo.org/3.2"
IMG_URL = BASE + "/rest-services/published-data/images/{country}/{pub}/{kind}/fullimage.pdf"

OUT_DIR = "front_pages"
LOG_PATH = "download_log.csv"

COUNTRY = "EP"

# Start conservative. If stable (few/no 429), increase slowly.
RATE_PER_SEC = 1.0         # global max requests per second
WORKERS = 1                # small parallelism, if needed increase after testing with 1
CHUNK_SIZE = 100           # tasks per chunk (tune for your machine)

# If OPS expects different page-range syntax, change here:
RANGE_HEADER_VALUE = "1"   # try "1" or "pages=1"


# ------------- RATE LIMITER -------------
class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self.min_interval = 1.0 / max(rate_per_sec, 0.0001)
        self.lock = threading.Lock()
        self.last = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            wait_for = self.min_interval - (now - self.last)
            if wait_for > 0:
                time.sleep(wait_for)
            self.last = time.time()

rate_limiter = RateLimiter(RATE_PER_SEC)


# ------------- TOKEN MANAGEMENT -------------
_token_lock = threading.Lock()
_token: Optional[str] = None

def _get_token_raw() -> str:
    r = requests.post(
        BASE + "/auth/accesstoken",
        data={"grant_type": "client_credentials"},
        auth=HTTPBasicAuth(OPS_KEY, OPS_SECRET),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def get_token_cached() -> str:
    global _token
    with _token_lock:
        if _token is None:
            _token = _get_token_raw()
        return _token

def refresh_token():
    global _token
    with _token_lock:
        _token = _get_token_raw()


# ------------- TASK MODEL -------------
@dataclass(frozen=True)
class DownloadTask:
    pub_number: str   # e.g. "0884389"
    kind: str         # e.g. "A1"
    country: str = COUNTRY


# ------------- LOGGING -------------
_log_lock = threading.Lock()

def init_log(path: str):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts", "country", "pub_number", "kind",
                "status", "http_status", "bytes", "message", "out_path"
            ])

def append_log(row: List):
    with _log_lock:
        with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)


# ------------- UTIL -------------
RETRY_STATUS = {429, 500, 502, 503, 504}

def out_path_for(task: DownloadTask) -> str:
    return os.path.join(OUT_DIR, f"{task.country}{task.pub_number}{task.kind}_page1.pdf")

def chunked(it: List[DownloadTask], size: int) -> Iterator[List[DownloadTask]]:
    for i in range(0, len(it), size):
        yield it[i:i+size]


# ------------- DOWNLOADER CORE -------------
def download_one(task: DownloadTask, session: requests.Session) -> Tuple[DownloadTask, bool, str, int, int, str]:
    """
    Returns: (task, success, status_str, http_status, bytes_written, out_path)
    status_str is one of: downloaded / skipped / failed
    """
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = out_path_for(task)

    # Skip if already downloaded and non-trivial size
    if os.path.exists(out_path) and os.path.getsize(out_path) > 1024:
        return task, True, "skipped", 200, os.path.getsize(out_path), out_path

    url = IMG_URL.format(country=task.country, pub=task.pub_number, kind=task.kind)

    headers = {
        "Authorization": f"Bearer {get_token_cached()}",
        "Accept": "application/pdf",
        "Range": RANGE_HEADER_VALUE,  # adjust here if needed
    }

    last_msg = ""
    http_status = 0

    for attempt in range(1, 8):  # up to 7 tries
        rate_limiter.wait()
        try:
            r = session.get(url, headers=headers, timeout=90)
            http_status = r.status_code

            # Token expired/invalid
            if r.status_code == 401:
                refresh_token()
                headers["Authorization"] = f"Bearer {get_token_cached()}"
                last_msg = "token refreshed"
                continue

            # Retryable errors
            if r.status_code in RETRY_STATUS:
                ra = r.headers.get("Retry-After")
                if ra and ra.isdigit():
                    sleep_s = int(ra)
                else:
                    sleep_s = min(60, (2 ** (attempt - 1)) + random.random())
                last_msg = f"retryable HTTP {r.status_code}, sleeping {sleep_s:.1f}s"
                time.sleep(sleep_s)
                continue

            # Hard fail
            if r.status_code != 200:
                # Often OPS returns XML error; keep a short snippet
                snippet = ""
                try:
                    snippet = r.text[:200].replace("\n", " ")
                except Exception:
                    snippet = "non-text body"
                last_msg = f"HTTP {r.status_code}: {snippet}"
                return task, False, "failed", r.status_code, 0, out_path

            # Sanity check: PDF header
            if not r.content.startswith(b"%PDF"):
                last_msg = f"not a PDF; first bytes={r.content[:20]!r}"
                return task, False, "failed", r.status_code, 0, out_path

            with open(out_path, "wb") as f:
                f.write(r.content)

            return task, True, "downloaded", r.status_code, len(r.content), out_path

        except requests.RequestException as e:
            sleep_s = min(60, (2 ** (attempt - 1)) + random.random())
            last_msg = f"request error: {e}; sleeping {sleep_s:.1f}s"
            time.sleep(sleep_s)

    return task, False, "failed", http_status, 0, out_path


def run_bulk(tasks: List[DownloadTask], workers: int = WORKERS, chunk_size: int = CHUNK_SIZE):
    init_log(LOG_PATH)
    os.makedirs(OUT_DIR, exist_ok=True)

    total = len(tasks)
    pbar = tqdm(total=total, desc="Downloading front pages", unit="file")

    with requests.Session() as session:
        # Process in chunks to control memory and be restart-friendly
        for chunk in chunked(tasks, chunk_size):
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(download_one, t, session) for t in chunk]

                for fut in as_completed(futures):
                    task, ok, status_str, http_status, nbytes, out_path = fut.result()

                    ts = time.strftime("%Y-%m-%d %H:%M:%S")
                    msg = "ok" if ok else "error"
                    append_log([
                        ts, task.country, task.pub_number, task.kind,
                        status_str, http_status, nbytes, msg, out_path
                    ])
                    pbar.update(1)

    pbar.close()
    print(f"Done. Log written to {LOG_PATH}. Files in {OUT_DIR}/")


# ---------------- INPUT HELPERS ----------------
def tasks_from_csv(path: str, pub_col: str = "pub_number", kind_col: str = "kind", country_col: Optional[str] = None) -> List[DownloadTask]:
    """
    CSV format example:
      pub_number,kind
      0884389,A1
      0995796,A2
    """
    tasks: List[DownloadTask] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pub = row[pub_col].strip()
            kind = row[kind_col].strip().upper()
            ctry = row[country_col].strip().upper() if country_col else COUNTRY
            tasks.append(DownloadTask(pub_number=pub, kind=kind, country=ctry))
    return tasks


if __name__ == "__main__":
    # Load from CSV with columns pub_number,kind
    tasks = tasks_from_csv("pub_number_kind.csv")  # <-- your input list here

    run_bulk(tasks, workers=WORKERS, chunk_size=CHUNK_SIZE)