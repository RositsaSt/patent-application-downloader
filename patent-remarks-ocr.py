from pdf2image import convert_from_path
import pytesseract
import cv2
import numpy as np
import re
from PIL import Image


def pdf_first_page_to_gray(pdf_path: str, dpi: int = 500) -> np.ndarray:
    page = convert_from_path(pdf_path, dpi=dpi, first_page=1, last_page=1)[0]
    rgb = np.array(page)
    gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)
    gray = cv2.convertScaleAbs(gray, alpha=1.25, beta=0)  # mild contrast
    return gray


def find_remarks_bbox(gray: np.ndarray):
    data = pytesseract.image_to_data(
        gray,
        output_type=pytesseract.Output.DICT,
        config="--oem 3 --psm 6"
    )
    best = None  # (conf, x, y, w, h)
    for i, txt in enumerate(data["text"]):
        if not txt:
            continue
        t = txt.strip().lower()
        # allow OCR variants like "Remarks", "Remarks:", "REMARKS"
        if re.fullmatch(r"remarks[:;]?", re.sub(r"[^a-z:;]", "", t)):
            conf = float(data["conf"][i]) if data["conf"][i] != "-1" else 0.0
            x = int(data["left"][i]); y = int(data["top"][i])
            w = int(data["width"][i]); h = int(data["height"][i])
            cand = (conf, x, y, w, h)
            if best is None or cand[0] > best[0]:
                best = cand
    return best


def find_horizontal_rule(gray: np.ndarray, x0: int, x1: int, y_start: int, y_end: int,
                         dark_thresh: int = 80, frac: float = 0.60):
    """
    Find the first strong horizontal line by scanning rows for a high fraction of dark pixels.
    """
    region = gray[y_start:y_end, x0:x1]
    dark = region < dark_thresh
    frac_dark = dark.mean(axis=1)
    idx = np.where(frac_dark > frac)[0]
    return (y_start + int(idx[0])) if len(idx) else None


def ocr_remarks_region(gray: np.ndarray) -> str | None:
    h, w = gray.shape[:2]
    bbox = find_remarks_bbox(gray)
    if bbox is None:
        return None

    _, x, y, bw, bh = bbox

    # Crop geometry: right-column box under "Remarks:"
    left = max(0, x - int(0.02 * w))
    right = min(w, left + int(0.45 * w))

    # IMPORTANT: don’t start too low (or you clip the first line)
    top = max(0, y + bh - 5)

    # Find the horizontal rule under the remarks box to stop cleanly
    search_end = min(h, top + int(0.25 * h))
    rule_y = find_horizontal_rule(gray, left, right, y + bh, search_end)

    bottom = rule_y if rule_y else min(h, top + int(0.12 * h))

    crop = gray[top:bottom, left:right]

    # OCR the crop
    txt = pytesseract.image_to_string(crop, config="--oem 3 --psm 6")
    txt = txt.replace("\r", "")
    # Clean: drop INID headers if they leak, and collapse whitespace
    txt = re.sub(r"\(\s*\d{2}\s*\).*", "", txt, flags=re.DOTALL).strip()
    txt = re.sub(r"\s+", " ", txt).strip()

    return txt or None


def extract_remarks_from_patent_pdf(pdf_path: str) -> str | None:
    gray = pdf_first_page_to_gray(pdf_path, dpi=500)
    return ocr_remarks_region(gray)


if __name__ == "__main__":
    print(extract_remarks_from_patent_pdf("EP0882737A1_page1.pdf"))
