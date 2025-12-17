"""pg_data.py

Helper untuk baca data dari PostgreSQL (SATGAS) TANPA merusak code lama yang masih pakai Google Sheets.

Dipakai untuk:
1) status kab/kota (public.data_status_kabkota)
2) GeoJSON polygon kab/kota (public.geo_kabkota) -> ditulis ke static/data/kabkota_sumut.json

ENV wajib:
  - DATABASE_URL   (contoh: postgresql://user:pass@host:5432/satgas_db)

ENV opsional:
  - PG_STATUS_TABLE   default: public.data_status_kabkota
  - PG_GEO_TABLE      default: public.geo_kabkota
  - GEOJSON_TTL_SECONDS default: 86400 (1 hari)
  - FORCE_GEOJSON_REFRESH default: 0
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import datetime as _dt
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return default
    return v


# ------------------------------------------------------------------------------
# Driver selection (psycopg v3 -> psycopg2)
# ------------------------------------------------------------------------------
_DRIVER = None
_HAS_PSYCOPG3 = False

try:
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore

    _DRIVER = "psycopg"
    _HAS_PSYCOPG3 = True
except Exception:
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore

if _DRIVER is None:
    try:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore

        _DRIVER = "psycopg2"
    except Exception:
        psycopg2 = None  # type: ignore


def _get_dsn() -> str:
    dsn = _get_env("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL belum diset. Set di .env / environment.")
    return dsn


def pg_fetchall(sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
    """Jalankan query dan return list of dict.

    Catatan:
    - Tidak pakai pool (simpel dulu).
    - Tiap call buka-konek-tutup (aman untuk awal).
    """
    dsn = _get_dsn()

    if _DRIVER == "psycopg":
        assert psycopg is not None  # noqa
        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                rows = cur.fetchall()
                return [dict(r) for r in rows]

    if _DRIVER == "psycopg2":
        assert psycopg2 is not None  # noqa
        with psycopg2.connect(dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params or ())
                rows = cur.fetchall()
                return [dict(r) for r in rows]

    raise RuntimeError(
        "Driver PostgreSQL tidak ditemukan. Install salah satu: psycopg[binary] atau psycopg2-binary."
    )


def pg_execute(sql: str, params: Optional[Tuple[Any, ...]] = None) -> int:
    """Jalankan statement (INSERT/UPDATE/DELETE) dan return rowcount."""
    dsn = _get_dsn()

    if _DRIVER == "psycopg":
        assert psycopg is not None  # noqa
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                return int(cur.rowcount or 0)

    if _DRIVER == "psycopg2":
        assert psycopg2 is not None  # noqa
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                return int(cur.rowcount or 0)

    raise RuntimeError(
        "Driver PostgreSQL tidak ditemukan. Install salah satu: psycopg[binary] atau psycopg2-binary."
    )



# ------------------------------------------------------------------------------
# JSON-safety helpers
# ------------------------------------------------------------------------------
def _json_safe_value(v: Any) -> Any:
    """Convert values returned from psycopg to JSON-serializable types.

    - Decimal -> float
    - datetime/date -> ISO string
    - Path -> str
    - dict/list/tuple -> recursively converted
    """
    try:
        if v is None:
            return None
        if isinstance(v, Decimal):
            # Use float for compatibility with json.dumps/jsonify
            return float(v)
        if isinstance(v, (_dt.datetime, _dt.date)):
            return v.isoformat()
        if isinstance(v, Path):
            return str(v)
        if isinstance(v, (list, tuple)):
            return [_json_safe_value(x) for x in v]
        if isinstance(v, dict):
            return {k: _json_safe_value(val) for k, val in v.items()}
        return v
    except Exception:
        # Fallback: stringify unknown types
        try:
            return str(v)
        except Exception:
            return None


# ------------------------------------------------------------------------------
# 1) Status Map (kab/kota -> status)
# ------------------------------------------------------------------------------
def pg_get_status_map() -> Dict[str, str]:
    """Ambil status terbaru per kab/kota dari tabel data_status_kabkota.

    Return format sama dengan code lama:
      { "KABUPATEN KARO": "Siaga Darurat", ... }
    """
    status_table = _get_env("PG_STATUS_TABLE", "public.data_status_kabkota")

    sql = f"""
        SELECT DISTINCT ON (upper(trim(nama_kabkota)))
            upper(trim(nama_kabkota)) AS kabkota_key,
            status_bencana
        FROM {status_table}
        WHERE nama_kabkota IS NOT NULL
          AND status_bencana IS NOT NULL
        ORDER BY upper(trim(nama_kabkota)), waktu DESC NULLS LAST;
    """

    rows = pg_fetchall(sql)
    out: Dict[str, str] = {}
    for r in rows:
        k = (r.get("kabkota_key") or "").strip()
        v = (r.get("status_bencana") or "").strip()
        if k and v:
            out[k] = v
    return out


# ------------------------------------------------------------------------------
# 2) GeoJSON kab/kota dari PostGIS
# ------------------------------------------------------------------------------
def pg_get_kabkota_featurecollection() -> Dict[str, Any]:
    """Ambil polygon kab/kota dari tabel geo_kabkota, return GeoJSON FeatureCollection."""
    geo_table = _get_env("PG_GEO_TABLE", "public.geo_kabkota")

    # Force2D + transform ke EPSG:4326 kalau SRID valid dan bukan 4326
    sql = f"""
        SELECT
            ogc_fid,
            kabkota,
            ST_AsGeoJSON(
                CASE
                    WHEN ST_SRID(geom) IN (0, 4326) THEN ST_Force2D(geom)
                    ELSE ST_Transform(ST_Force2D(geom), 4326)
                END
            )::json AS geometry
        FROM {geo_table}
        WHERE geom IS NOT NULL;
    """

    rows = pg_fetchall(sql)
    features: List[Dict[str, Any]] = []
    for r in rows:
        geom = r.get("geometry")
        kabkota = r.get("kabkota")
        if not geom or not kabkota:
            continue

        features.append(
            {
                "type": "Feature",
                "geometry": geom,
                "properties": {
                    "ogc_fid": r.get("ogc_fid"),
                    "kabkota": kabkota,
                },
            }
        )

    return {"type": "FeatureCollection", "features": features}


def ensure_kabkota_geojson_static(app_root_path: str) -> Path:
    """Pastikan static/data/kabkota_sumut.json tersedia.

    - Kalau file tidak ada / sudah expired TTL -> generate ulang dari Postgres.
    - Ini dibuat agar map.html / JS lama yang load static JSON tetap jalan tanpa ubah front-end.
    """
    ttl = int(_get_env("GEOJSON_TTL_SECONDS", "86400") or "86400")
    force = str(_get_env("FORCE_GEOJSON_REFRESH", "0")).strip() in ("1", "true", "True", "YES", "yes")

    out_path = Path(app_root_path) / "static" / "data" / "kabkota_sumut.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and not force:
        age = (Path(out_path).stat().st_mtime)
        import time
        if time.time() - age < ttl:
            return out_path

    fc = pg_get_kabkota_featurecollection()
    out_path.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
    return out_path


# ------------------------------------------------------------------------------
# 3) Data relawan (untuk login / tambah data)
# ------------------------------------------------------------------------------
def pg_get_relawan_list() -> List[Dict[str, Any]]:
    """Ambil daftar relawan dari tabel Postgres (default: public.data_relawan).

    Output disamakan dengan struktur Google Sheet 'data_relawan':
      - id_relawan
      - nama_relawan
      - kode_akses
    """
    relawan_table = _get_env("PG_RELAWAN_TABLE", "public.data_relawan")

    sql = f"""
        SELECT
            id_relawan,
            nama_relawan,
            kode_akses
        FROM {relawan_table}
        ORDER BY upper(trim(nama_relawan)) ASC;
    """

    rows = pg_fetchall(sql)
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append({
            "id_relawan": (r.get("id_relawan") or "").strip(),
            "nama_relawan": (r.get("nama_relawan") or "").strip(),
            "kode_akses": (r.get("kode_akses") or "").strip(),
        })

    return [x for x in out if x.get("nama_relawan")]


# ------------------------------------------------------------------------------
# 3b) Data lokasi (marker di peta + dropdown posko)
# ------------------------------------------------------------------------------
def pg_get_data_lokasi_list() -> List[Dict[str, Any]]:
    """Ambil daftar lokasi dari tabel Postgres (default: public.data_lokasi).

    Tujuan: output *menyerupai* struktur Google Sheet 'data_lokasi' supaya map.html / JS lama
    tidak perlu diubah.

    Mapping utama:
      - id_lokasi   -> kode_lokasi
      - nama_kabkota -> kabupaten_kota

    Kolom lain (jenis_lokasi, status_lokasi, tingkat_akses, kondisi, nama_lokasi, alamat,
    latitude, longitude, dll) diteruskan jika ada.
    """
    lokasi_table = _get_env("PG_DATA_LOKASI_TABLE", "public.data_lokasi")

    # Ambil semua kolom agar fleksibel (kalau nanti tabel tambah kolom)
    sql = f"""SELECT * FROM {lokasi_table};"""
    rows = pg_fetchall(sql)

    out: List[Dict[str, Any]] = []
    for r in rows:
        # Ambil dari berbagai kemungkinan nama kolom
        kode = r.get("kode_lokasi") or r.get("id_lokasi") or r.get("kode") or r.get("id")
        kabkota = r.get("kabupaten_kota") or r.get("nama_kabkota") or r.get("kabkota")

        # Buat payload kompatibel dengan sheet
        item = dict(r)  # keep original keys too
        for _k, _v in list(item.items()):
            item[_k] = _json_safe_value(_v)
        if kode is not None:
            item["kode_lokasi"] = str(kode).strip()
            item["id_lokasi"] = str(kode).strip()
        if kabkota is not None:
            item["kabupaten_kota"] = str(kabkota).strip()
            item["nama_kabkota"] = str(kabkota).strip()

        # Normalisasi string fields (kalau ada)
        for k in ("jenis_lokasi","status_lokasi","tingkat_akses","kondisi","nama_lokasi","alamat"):
            if k in item and item[k] is not None:
                item[k] = str(item[k]).strip()

        # lat/lon biarkan numeric atau string, yang penting truthy saat ada
        out.append(item)

    # Optional sort agar konsisten (posko dulu, lalu alfabet)
    try:
        out.sort(key=lambda x: (str(x.get("jenis_lokasi") or ""), str(x.get("kabupaten_kota") or ""), str(x.get("kode_lokasi") or "")))
    except Exception:
        pass

    return out

# ------------------------------------------------------------------------------
# 4) Insert Absensi Lokasi Relawan -> tabel public.lokasi_relawan
# ------------------------------------------------------------------------------
def _to_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        s = str(val).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def pg_insert_lokasi_relawan(
    *,
    id_relawan: str,
    latitude: Any,
    longitude: Any,
    catatan: Optional[str] = None,
    waktu: Optional[_dt.datetime] = None,
    lokasi_text: Optional[str] = None,
    photo_link: Optional[str] = None,
    lokasi_posko: Optional[str] = None,
) -> bool:
    """Simpan absensi relawan ke PostgreSQL.

    Minimal yang dipakai sekarang:
      - latitude -> kolom latitude
      - longitude -> kolom longitude
      - catatan -> kolom catatan

    Kolom lain masih boleh NULL.
    """
    table = _get_env("PG_LOKASI_RELAWAN_TABLE", "public.lokasi_relawan")

    lat = _to_float(latitude)
    lon = _to_float(longitude)
    if lat is None or lon is None:
        raise ValueError("latitude/longitude tidak valid")

    if waktu is None:
        waktu = _dt.datetime.now(_dt.timezone.utc)

    sql = f"""
        INSERT INTO {table}
            (id_relawan, waktu, latitude, longitude, catatan, lokasi_text, photo_link, lokasi_posko)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    rowcount = pg_execute(
        sql,
        (
            str(id_relawan or "").strip(),
            waktu,
            lat,
            lon,
            (catatan or "").strip() if catatan is not None else None,
            (lokasi_text or "").strip() if lokasi_text is not None else None,
            (photo_link or "").strip() if photo_link is not None else None,
            (lokasi_posko or "").strip() if lokasi_posko is not None else None,
        ),
    )
    return rowcount >= 1
