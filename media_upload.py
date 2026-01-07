# media_upload.py
import os
import re
import json
import datetime as dt
from typing import List, Optional
from werkzeug.utils import secure_filename

ALLOWED_EXT = {".jpg", ".jpeg", ".png", ".webp"}

def _slug(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-]", "", s)
    return s or "unknown"

def save_asesmen_photos(
    files,
    asesmen_name: str,
    id_relawan: str,
    media_root: str,
    waktu_utc: Optional[dt.datetime] = None,
    max_files: int = 2,
) -> List[str]:
    """Return list of relative paths, e.g. ['asesmen/kesehatan_RR001_20251223_083000_1.jpg', ...]"""
    if not files:
        return []

    picked = [f for f in files if f and getattr(f, "filename", "")]  # skip empty
    if not picked:
        return []
    if len(picked) > max_files:
        raise ValueError(f"Maksimal {max_files} foto.")

    asesmen_name = _slug(asesmen_name)
    id_relawan = _slug(id_relawan)

    waktu_utc = waktu_utc or dt.datetime.utcnow()
    ts = waktu_utc.strftime("%Y%m%d_%H%M%S")

    out_dir = os.path.join(media_root, "asesmen")
    os.makedirs(out_dir, exist_ok=True)

    saved_rel = []
    for i, f in enumerate(picked, start=1):
        original = secure_filename(f.filename) or f"foto_{i}.jpg"
        ext = os.path.splitext(original)[1].lower()
        if ext not in ALLOWED_EXT:
            raise ValueError("Format foto harus JPG/JPEG/PNG/WEBP.")

        filename = f"{asesmen_name}_{id_relawan}_{ts}_{i}{ext}"
        abs_path = os.path.join(out_dir, filename)
        f.save(abs_path)

        rel_path = os.path.join("asesmen", filename).replace("\\", "/")
        saved_rel.append(rel_path)

    return saved_rel

def photos_to_photo_path_value(paths: List[str]) -> Optional[str]:
    """
    Simpan ke kolom photo_path.
    Rekomendasi: simpan sebagai JSON string array agar 1 kolom bisa menampung max 2 foto.
    """
    if not paths:
        return None
    return json.dumps(paths, ensure_ascii=False)
