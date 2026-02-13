from __future__ import annotations

import csv
import tarfile
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, List, Optional, Tuple


# ---------------- CONFIG ----------------
TOP_ARCHIVES_DIR = Path("tmp_bdds")          # your downloaded top-level archives
OUT_CSV = Path("b053ep_extracted.csv")
TARGET_TAG = "B053EP"
A1_SUFFIX = "A1"

ARCHIVE_EXTS = (
    ".zip",
    ".tar", ".tar.gz", ".tgz",
    ".tar.bz2", ".tbz2",
    ".tar.xz", ".txz",
)


# ---------------- ARCHIVE HELPERS ----------------

def is_archive(p: Path) -> bool:
    name = p.name.lower()
    return any(name.endswith(ext) for ext in ARCHIVE_EXTS)

def extract_archive(archive_path: Path, extract_to: Path) -> None:
    """
    Extract .zip or .tar.* into extract_to.
    """
    extract_to.mkdir(parents=True, exist_ok=True)
    lower = archive_path.name.lower()

    if lower.endswith(".zip"):
        with zipfile.ZipFile(archive_path) as z:
            z.extractall(extract_to)
        return

    if lower.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")):
        with tarfile.open(archive_path, mode="r:*") as t:
            t.extractall(extract_to)
        return

    raise ValueError(f"Unsupported archive: {archive_path}")


# ---------------- XML PARSING ----------------

def strip_ns(tag: str) -> str:
    """Turn '{namespace}B053EP' into 'B053EP'."""
    return tag.split("}", 1)[-1] if "}" in tag else tag

def extract_tag_values(xml_path: Path, tag_name: str) -> List[str]:
    """
    Streaming parse; returns all non-empty text values inside <tag_name>.
    """
    values: List[str] = []
    try:
        for _, elem in ET.iterparse(xml_path, events=("end",)):
            if strip_ns(elem.tag) == tag_name:
                text = (elem.text or "").strip()
                if text:
                    values.append(text)
            elem.clear()
    except Exception:
        return []
    return values


# ---------------- STRUCTURE FINDING ----------------

def find_doc_dir(root: Path) -> Optional[Path]:
    for p in root.rglob("DOC"):
        if p.is_dir():
            return p
    return None

def find_a1_dirs(doc_dir: Path) -> List[Path]:
    return [p for p in doc_dir.rglob("*") if p.is_dir() and p.name.endswith(A1_SUFFIX)]

def find_first_xml(root: Path) -> Optional[Path]:
    xmls = sorted(root.rglob("*.xml"))
    return xmls[0] if xmls else None


# ---------------- NESTED PROCESSING ----------------

def process_inner_archive(inner_archive: Path) -> Tuple[Optional[Path], List[str]]:
    """
    Extract one inner archive to a temp dir, find XML, return (xml_path, values).
    Everything extracted is deleted automatically when the function returns.
    """
    with TemporaryDirectory() as tmp_inner:
        tmp_inner = Path(tmp_inner)
        extract_archive(inner_archive, tmp_inner)

        xml_path = find_first_xml(tmp_inner)
        if not xml_path:
            return None, []

        values = extract_tag_values(xml_path, TARGET_TAG)
        return xml_path, values

def process_top_archive(top_archive: Path) -> List[Tuple[str, str, str, str]]:
    """
    Returns rows:
      (top_archive_name, a1_dir_name, inner_archive_name, B053EP_value)

    All extracted files (top + inner) are deleted automatically.
    """
    rows: List[Tuple[str, str, str, str]] = []

    with TemporaryDirectory() as tmp_top:
        tmp_top = Path(tmp_top)
        extract_archive(top_archive, tmp_top)

        doc_dir = find_doc_dir(tmp_top)
        if not doc_dir:
            print(f"[warn] No DOC dir found in {top_archive.name}")
            return rows

        a1_dirs = find_a1_dirs(doc_dir)
        if not a1_dirs:
            print(f"[warn] No *{A1_SUFFIX} dir found under DOC for {top_archive.name}")
            return rows

        for a1_dir in a1_dirs:
            # Inner archives are *inside* the A1 folder (you described this exactly)
            inner_archives = sorted(p for p in a1_dir.iterdir() if p.is_file() and is_archive(p))
            if not inner_archives:
                print(f"[warn] No inner archives found in {a1_dir} (from {top_archive.name})")
                continue

            for inner in inner_archives:
                xml_path, values = process_inner_archive(inner)
                if not values:
                    continue

                for v in values:
                    rows.append((top_archive.name, a1_dir.name, inner.name, v))

    return rows


# ---------------- MAIN ----------------

def main():
    top_archives = sorted(p for p in TOP_ARCHIVES_DIR.iterdir() if p.is_file() and is_archive(p))
    if not top_archives:
        raise SystemExit(f"No archives found in {TOP_ARCHIVES_DIR.resolve()}")

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["top_archive", "a1_dir", "inner_archive", "B053EP"])

        total = 0
        for ta in top_archives:
            print(f"Top archive: {ta.name}")
            rows = process_top_archive(ta)
            for r in rows:
                w.writerow(r)
            total += len(rows)
            print(f"  -> extracted {len(rows)} values")

    print(f"Done. Wrote {total} rows to {OUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
