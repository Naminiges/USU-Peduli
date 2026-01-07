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

def save_lokasi_photo(
    file,
    id_lokasi: str,
    media_root: str,
) -> Optional[str]:
    """Simpan 1 foto lokasi ke media/photo_lokasi/<ID_LOKASI>.jpg/png/...

    Return: nilai untuk kolom photo_path, mis. "media/photo_lokasi/G-TT001.jpg"
    """
    if not file or not getattr(file, "filename", ""):
        return None

    sid = _slug(id_lokasi)

    original = secure_filename(file.filename) or "foto.jpg"
    ext = os.path.splitext(original)[1].lower()
    if ext == ".jpeg":
        ext = ".jpg"
    if ext not in ALLOWED_EXT:
        raise ValueError("Format foto harus JPG/JPEG/PNG/WEBP.")

    out_dir = os.path.join(media_root, "photo_lokasi")
    os.makedirs(out_dir, exist_ok=True)

    filename = f"{sid}{ext}"
    abs_path = os.path.join(out_dir, filename)
    file.save(abs_path)

    # Disimpan sebagai path relatif yang konsisten dipanggil dari front-end (prefix: media/)
    rel_path = os.path.join("media", "photo_lokasi", filename).replace("\\", "/")
    return rel_path

def photos_to_photo_path_value(paths: List[str]) -> Optional[str]:
    """
    Simpan ke kolom photo_path.
    Rekomendasi: simpan sebagai JSON string array agar 1 kolom bisa menampung max 2 foto.
    """
    if not paths:
        return None
    return json.dumps(paths, ensure_ascii=False)
