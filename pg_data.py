"""pg_data.py

Helper untuk baca/tulis data dari PostgreSQL (SATGAS) TANPA merusak gaya code app.

Dipakai untuk:
1) status kab/kota (public.data_status_kabkota) -> status_map untuk fill polygon
2) GeoJSON polygon kab/kota (public.geo_kabkota) -> ditulis ke static/data/kabkota_sumut.json
3) data_lokasi (public.data_lokasi) -> marker posko/lokasi
4) data_relawan (public.data_relawan) -> login
5) lokasi_relawan (public.lokasi_relawan) -> submit absensi lokasi
6) asesmen_kesehatan (public.asesmen_kesehatan)
7) asesmen_pendidikan (public.asesmen_pendidikan)

Tambahan (opsional, jika tabel ada di Postgres):
8) stok_gudang (public.stok_gudang) -> tabel stok untuk UI
9) master_logistik (public.master_logistik) -> master kode_barang untuk dropdown
10) rekapitulasi_data_kabkota (public.rekapitulasi_data_kabkota) -> rekap per kab/kota
11) permintaan_posko (public.permintaan_posko) -> submit permintaan dari posko

ENV wajib:
  - DATABASE_URL   (contoh: postgresql://user:pass@host:5432/satgas_db)

ENV opsional:
  - PG_STATUS_TABLE              default: public.data_status_kabkota
  - PG_GEO_TABLE                 default: public.geo_kabkota
  - PG_DATA_LOKASI_TABLE         default: public.data_lokasi
  - PG_RELAWAN_TABLE             default: public.data_relawan
  - PG_LOKASI_RELAWAN_TABLE      default: public.lokasi_relawan
  - PG_ASESMEN_KESEHATAN_TABLE   default: public.asesmen_kesehatan
  - PG_ASESMEN_PENDIDIKAN_TABLE  default: public.asesmen_pendidikan

  - PG_STOK_GUDANG_TABLE         default: public.stok_gudang
  - PG_MASTER_LOGISTIK_TABLE     default: public.master_logistik
  - PG_REKAP_KABKOTA_TABLE       default: public.rekapitulasi_data_kabkota
  - PG_PERMINTAAN_POSKO_TABLE    default: public.permintaan_posko

  - GEOJSON_TTL_SECONDS          default: 86400 (1 hari)
  - FORCE_GEOJSON_REFRESH        default: 0

Catatan:
- Kompatibel dengan psycopg v3 (psycopg) maupun psycopg2.
- Untuk awal, tiap call query buka-konek-tutup (simpel & aman).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Sequence, Union
import re

# ------------------------------------------------------------------------------
# Driver selection (psycopg v3 -> psycopg2)
# ------------------------------------------------------------------------------
_DRIVER: Optional[str] = None

try:
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore

    _DRIVER = "psycopg"
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


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return default
    return v


def _get_dsn() -> str:
    dsn = _get_env("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL belum diset. Set di .env / environment.")
    return dsn


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, Decimal):
            return float(v)
        return float(str(v).strip())
    except Exception:
        return None

def pg_fetchone(sql: str, params: Optional[Tuple[Any, ...]] = None) -> Optional[Dict[str, Any]]:
    rows = pg_fetchall(sql, params)
    return rows[0] if rows else None

def _json_safe_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        # Kalau datetime bertz (umumnya dari timestamptz), normalkan ke UTC naive
        if v.tzinfo is not None and v.utcoffset() is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v.isoformat()
    return v


def _json_safe_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _json_safe_value(v) for k, v in row.items()}


# ------------------------------------------------------------------------------
# Time helper (WIB / GMT+7)
# ------------------------------------------------------------------------------
_WIB = ZoneInfo("Asia/Jakarta")

def _now_wib_naive() -> datetime:
    """Return waktu saat ini dalam WIB (GMT+7) tanpa tzinfo (naive).

    Dipakai agar nilai waktu yang tampil di UI sesuai WIB tanpa bergantung timezone server.
    """
    return datetime.now(_WIB).replace(tzinfo=None)



def pg_fetchall(sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
    """Jalankan query dan return list of dict."""
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


def pg_execute(sql: str, params: Optional[Tuple[Any, ...]] = None) -> None:
    """Execute (INSERT/UPDATE/DELETE)."""
    dsn = _get_dsn()

    if _DRIVER == "psycopg":
        assert psycopg is not None  # noqa
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
            conn.commit()
        return

    if _DRIVER == "psycopg2":
        assert psycopg2 is not None  # noqa
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
            conn.commit()
        return

    raise RuntimeError(
        "Driver PostgreSQL tidak ditemukan. Install salah satu: psycopg[binary] atau psycopg2-binary."
    )


# ------------------------------------------------------------------------------
# 1) Status Map (kab/kota -> status)
# ------------------------------------------------------------------------------
def pg_get_status_map() -> Dict[str, str]:
    """Ambil status terbaru per kab/kota dari tabel data_status_kabkota.

    Return format kompatibel dengan app:
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
    - Ini dibuat agar front-end yang load static JSON tetap jalan tanpa ubah HTML/JS.
    """
    ttl = int(_get_env("GEOJSON_TTL_SECONDS", "86400") or "86400")
    force = str(_get_env("FORCE_GEOJSON_REFRESH", "0")).strip() in ("1", "true", "True", "YES", "yes")

    out_path = Path(app_root_path) / "static" / "data" / "kabkota_sumut.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and not force:
        import time
        age = out_path.stat().st_mtime
        if time.time() - age < ttl:
            return out_path

    fc = pg_get_kabkota_featurecollection()
    out_path.write_text(json.dumps(fc, ensure_ascii=False), encoding="utf-8")
    return out_path


# ------------------------------------------------------------------------------
# 3) data_lokasi (marker)
# ------------------------------------------------------------------------------
def pg_get_data_lokasi() -> List[Dict[str, Any]]:
    """Ambil data_lokasi dari Postgres.

    Catatan:
    - Default hanya dipakai untuk marker & dropdown; filtering is_active dilakukan di layer app/map.
    - Kolom is_active ditambahkan belakangan. Fungsi ini dibuat kompatibel (fallback bila kolom belum ada).
    """
    table = _get_env("PG_DATA_LOKASI_TABLE", "public.data_lokasi")

    # Query baru (dengan is_active)
    sql_new = f"""
        SELECT
            id_lokasi,
            jenis_lokasi,
            nama_kabkota,
            nama_lokasi,
            status_lokasi,
            tingkat_akses,
            kondisi,
            catatan,
            photo_path,
            latitude,
            longitude,
            waktu,
            COALESCE(is_active, TRUE) AS is_active
        FROM {table}
        ORDER BY waktu DESC;
    """

    # Query lama (tanpa is_active)
    sql_old = f"""
        SELECT
            id_lokasi,
            jenis_lokasi,
            nama_kabkota,
            nama_lokasi,
            status_lokasi,
            tingkat_akses,
            kondisi,
            catatan,
            photo_path,
            latitude,
            longitude,
            waktu
        FROM {table}
        ORDER BY waktu DESC;
    """

    try:
        rows = pg_fetchall(sql_new)
    except Exception:
        rows = pg_fetchall(sql_old)

    out: List[Dict[str, Any]] = []
    for r in rows:
        lat = _to_float(r.get("latitude"))
        lon = _to_float(r.get("longitude"))

        is_active = r.get("is_active")
        if isinstance(is_active, str):
            is_active_val = is_active.strip().lower() in ("1", "true", "t", "yes", "y", "on")
        elif is_active is None:
            is_active_val = True
        else:
            is_active_val = bool(is_active)

        # ✅ FIX: pastikan waktu JSON-safe (datetime -> iso string)
        waktu_val = r.get("waktu")
        if hasattr(waktu_val, "isoformat"):
            waktu_val = waktu_val.isoformat()  # contoh: "2025-12-31T08:57:00+07:00"

        out.append(
            {
                "id_lokasi": str(r.get("id_lokasi") or ""),
                "jenis_lokasi": str(r.get("jenis_lokasi") or ""),
                "nama_kabkota": str(r.get("nama_kabkota") or ""),
                "nama_lokasi": str(r.get("nama_lokasi") or ""),
                "status_lokasi": str(r.get("status_lokasi") or ""),
                "tingkat_akses": str(r.get("tingkat_akses") or ""),
                "kondisi": str(r.get("kondisi") or ""),
                "catatan": str(r.get("catatan") or ""),
                "photo_path": str(r.get("photo_path") or ""),
                "latitude": lat,
                "longitude": lon,
                "waktu": waktu_val,          # ✅ sekarang aman untuk jsonify/json.dumps
                "is_active": is_active_val,
            }
        )

    return out


# ------------------------------------------------------------------------------
# 3b) REF TABLES untuk dropdown INPUT LOKASI (opsional)
# ------------------------------------------------------------------------------
def _pg_ref_list(table: str, cols: Union[str, Sequence[str]]) -> List[str]:
    """Ambil 1 kolom (atau beberapa kandidat kolom) dari tabel referensi untuk dropdown.
    Return: list string unik (urut A-Z).
    """
    if isinstance(cols, str):
        cols = [cols]

    for col in cols:
        try:
            rows = pg_fetchall(
                f"SELECT {col} AS v FROM {table} WHERE {col} IS NOT NULL ORDER BY {col} ASC;"
            )
        except Exception:
            continue

        out: List[str] = []
        seen = set()
        for r in rows:
            v = (r.get("v") or "").strip()
            if not v:
                continue
            key = v.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(v)

        if out:
            return out

    return []


def pg_get_ref_jenis_lokasi() -> List[str]:
    table = _get_env("PG_REF_JENIS_LOKASI_TABLE", "public.ref_jenis_lokasi")
    return _pg_ref_list(table, ["nama", "jenis_lokasi", "value", "label"])

def pg_get_ref_kabkota() -> List[str]:
    table = _get_env("PG_REF_KABKOTA_TABLE", "public.ref_kabkota")
    return _pg_ref_list(table, ["nama_kabkota", "kabkota", "nama", "value", "label"])

def pg_get_ref_status_lokasi() -> List[str]:
    table = _get_env("PG_REF_STATUS_LOKASI_TABLE", "public.ref_status_lokasi")
    return _pg_ref_list(table, ["nama", "status_lokasi", "value", "label"])

def pg_get_ref_tingkat_akses() -> List[str]:
    table = _get_env("PG_REF_TINGKAT_AKSES_TABLE", "public.ref_tingkat_akses")
    return _pg_ref_list(table, ["nama", "tingkat_akses", "akses", "value", "label"])

def pg_get_ref_kondisi() -> List[str]:
    table = _get_env("PG_REF_KONDISI_TABLE", "public.ref_kondisi")
    return _pg_ref_list(table, ["nama", "kondisi", "kondisi_umum", "value", "label"])

# ------------------------------------------------------------------------------
# 3c) data_lokasi (insert) + AUTO ID (prefix + kabkota_code + increment)
# ------------------------------------------------------------------------------
_KABKOTA_CODE_MAP: Dict[str, str] = {
    "NIAS": "NI",
    "NIAS SELATAN": "NS",
    "NIAS UTARA": "NU",
    "NIAS BARAT": "NB",
    "GUNUNGSITOLI": "GS",

    "TAPANULI SELATAN": "TS",
    "TAPANULI TENGAH": "TT",
    "TAPANULI UTARA": "TU",
    "TOBA": "TB",
    "SAMOSIR": "SM",
    "HUMBANG HASUNDUTAN": "HH",
    "PAKPAK BHARAT": "PB",
    "DAIRI": "DA",
    "KARO": "KA",
    "SIBOLGA": "SI",

    "LANGKAT": "LA",
    "DELI SERDANG": "DS",
    "SERDANG BEDAGAI": "SB",
    "BATU BARA": "BB",
    "ASAHAN": "AS",
    "SIMALUNGUN": "SL",
    "PEMATANGSIANTAR": "PS",
    "TEBING TINGGI": "TG",
    "BINJAI": "BI",
    "MEDAN": "ME",

    "LABUHAN BATU": "LB",
    "LABUHANBATU": "LB",
    "LABUHAN BATU SELATAN": "LS",
    "LABUHANBATU SELATAN": "LS",
    "LABUHAN BATU UTARA": "LU",
    "LABUHANBATU UTARA": "LU",

    "MANDAILING NATAL": "MN",
    "PADANG LAWAS": "PL",
    "PADANG LAWAS UTARA": "PU",
    "TANJUNG BALAI": "TJ",
    "PADANGSIDEMPUAN": "PD",
    "PADANG SIDEMPUAN": "PD",

    "ACEH TAMIANG": "AT",
}

_JENIS_PREFIX_MAP: Dict[str, str] = {
    "POSKO PENGUNGSIAN": "P",
    "GUDANG LOGISTIK": "G",
    "STARLINK": "S",
    "JEMBATAN RUSAK": "JR",
    "JALAN PUTUS": "JP",
    "TITIK LONGSOR": "TL",
}

def _norm_kabkota(nama: Any) -> str:
    s = str(nama or "").strip()
    s = " ".join(s.split()).upper()
    for p in ("KABUPATEN ", "KOTA "):
        if s.startswith(p):
            s = s[len(p):].strip()
            break
    return s

def _kabkota_code(nama_kabkota: Any) -> str:
    key = _norm_kabkota(nama_kabkota)
    if key in _KABKOTA_CODE_MAP:
        return _KABKOTA_CODE_MAP[key]
    parts = [p for p in key.split(" ") if p]
    if len(parts) >= 2:
        return (parts[0][:1] + parts[1][:1]).upper()
    if len(parts) == 1:
        return parts[0][:2].upper()
    return "XX"

def _jenis_prefix(jenis_lokasi: Any) -> str:
    key = str(jenis_lokasi or "").strip().upper()
    if key in _JENIS_PREFIX_MAP:
        return _JENIS_PREFIX_MAP[key]
    return (key[:1] or "X").upper()

def pg_next_data_lokasi_id(jenis_lokasi: str, nama_kabkota: str) -> str:
    table = _get_env("PG_DATA_LOKASI_TABLE", "public.data_lokasi")
    prefix = _jenis_prefix(jenis_lokasi)
    kabcode = _kabkota_code(nama_kabkota)
    base = f"{prefix}-{kabcode}"
    like = base + "%"

    rows = pg_fetchall(
        f"SELECT id_lokasi FROM {table} WHERE id_lokasi LIKE %s ORDER BY id_lokasi DESC LIMIT 1;",
        (like,),
    )
    last_id = (rows[0].get("id_lokasi") if rows else "") or ""

    n = 1  # start from 001
    m = re.search(r"(\d{3})$", str(last_id))
    if m:
        try:
            n = int(m.group(1)) + 1
        except Exception:
            n = 1

    return f"{base}{n:03d}"

def pg_insert_data_lokasi(
    *,
    id_lokasi: Optional[str],
    jenis_lokasi: str,
    nama_kabkota: str,
    status_lokasi: str,
    tingkat_akses: str,
    kondisi: str,
    nama_lokasi: str,
    alamat: Optional[str] = None,
    kecamatan: Optional[str] = None,
    desa_kelurahan: Optional[str] = None,
    latitude: Any = None,
    longitude: Any = None,
    lokasi_text: Optional[str] = None,
    catatan: Optional[str] = None,
    pic: Optional[str] = None,
    pic_hp: Optional[str] = None,
    photo_path: Optional[str] = None,
) -> str:
    table = _get_env("PG_DATA_LOKASI_TABLE", "public.data_lokasi")

    final_id = (id_lokasi or "").strip()
    if not final_id:
        final_id = pg_next_data_lokasi_id(jenis_lokasi, nama_kabkota)

    lat_f = _to_float(latitude)
    lon_f = _to_float(longitude)

    sql = f"""
        INSERT INTO {table}
        (id_lokasi, jenis_lokasi, nama_kabkota, status_lokasi, tingkat_akses, kondisi,
         nama_lokasi, alamat, kecamatan, desa_kelurahan,
         latitude, longitude, lokasi_text, catatan, pic, pic_hp, photo_path)
        VALUES
        (%s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s);
    """

    pg_execute(sql, (
        final_id, jenis_lokasi, nama_kabkota, status_lokasi, tingkat_akses, kondisi,
        nama_lokasi, alamat, kecamatan, desa_kelurahan,
        lat_f, lon_f, lokasi_text, catatan, pic, pic_hp, photo_path
    ))

    return final_id



# ------------------------------------------------------------------------------
# 4) data_relawan (login)
# ------------------------------------------------------------------------------
def pg_get_relawan_list() -> List[Dict[str, Any]]:
    """Ambil relawan untuk login: id_relawan, nama_relawan, kode_akses."""
    table = _get_env("PG_RELAWAN_TABLE", "public.data_relawan")
    sql = f"""
        SELECT
            id_relawan,
            nama_relawan,
            kode_akses,
            COALESCE(is_admin, FALSE) AS is_admin
        FROM {table}
        WHERE nama_relawan IS NOT NULL
        ORDER BY nama_relawan ASC;
    """
    rows = pg_fetchall(sql)
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id_relawan": r.get("id_relawan"),
                "nama_relawan": r.get("nama_relawan"),
                "kode_akses": r.get("kode_akses"),
                "is_admin": r.get("is_admin"),
            }
        )
    return out


# ------------------------------------------------------------------------------
# 5) lokasi_relawan (absensi) - insert
# ------------------------------------------------------------------------------
def pg_insert_lokasi_relawan(
    id_relawan: str,
    latitude: Any,
    longitude: Any,
    catatan: Optional[str] = None,
    lokasi: Optional[str] = None,
    lokasi_posko: Optional[str] = None,
    photo_link: Optional[str] = None,
    waktu: Optional[datetime] = None,
) -> bool:
    table = _get_env("PG_LOKASI_RELAWAN_TABLE", "public.lokasi_relawan")

    lat_f = _to_float(latitude)
    lon_f = _to_float(longitude)

    if lat_f is None or lon_f is None:
        raise ValueError("latitude/longitude tidak valid")

    w = waktu or datetime.now(timezone.utc).replace(tzinfo=None)

    # Urutan percobaan (biar tahan beda nama kolom/atribut)
    attempts: List[Tuple[str, Tuple[Any, ...]]] = [
        (
            f"""
            INSERT INTO {table}
            (waktu, id_relawan, latitude, longitude, catatan, lokasi, lokasi_posko, photo_link)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (w, id_relawan, lat_f, lon_f, catatan, lokasi, lokasi_posko, photo_link),
        ),
        (
            f"""
            INSERT INTO {table}
            (waktu, id_relawan, latitude, longitude, catatan)
            VALUES (%s,%s,%s,%s,%s)
            """,
            (w, id_relawan, lat_f, lon_f, catatan),
        ),
        (
            f"""
            INSERT INTO {table}
            (timestamp, id_relawan, latitude, longitude, catatan)
            VALUES (%s,%s,%s,%s,%s)
            """,
            (w, id_relawan, lat_f, lon_f, catatan),
        ),
    ]

    last_err: Optional[Exception] = None
    for sql, params in attempts:
        try:
            pg_execute(sql, params)
            return True
        except Exception as e:
            last_err = e
            continue

    if last_err:
        raise last_err
    return False


# ------------------------------------------------------------------------------
# 6) asesmen_kesehatan / asesmen_pendidikan - insert
# ------------------------------------------------------------------------------
def _insert_asesmen(
    table_env: str,
    default_table: str,
    id_relawan: str,
    kode_posko: Optional[str],
    jawaban: Dict[str, Any],
    skor: float,
    status: str,
    latitude: Any,
    longitude: Any,
    catatan: Optional[str],
    photo_path: Optional[str] = None,
    radius: Optional[float] = None,
    waktu: Optional[datetime] = None,
    prefer_jsonb_cast: bool = True,
) -> bool:
    table = _get_env(table_env, default_table)

    lat_f = _to_float(latitude)
    lon_f = _to_float(longitude)
    w = waktu or datetime.now(timezone.utc).replace(tzinfo=None)

    rad_f = _to_float(radius)

    payload = json.dumps(jawaban, ensure_ascii=False)

    # Ada DB yang kolom jawaban-nya TEXT, ada yang jsonb. Kita coba 2 cara.
    attempts: List[Tuple[str, Tuple[Any, ...]]] = []

    # Jika kolom 'radius' ada (baru), coba insert dengan radius dulu.
    photo_v: Optional[str] = photo_path if photo_path not in (None, "") else None

    # Jika kolom 'radius' ada (baru), coba insert dengan radius dulu.
    if rad_f is not None:
        # radius + photo_path
        if photo_v is not None:
            if prefer_jsonb_cast:
                attempts.append(
                    (
                        f"""
                        INSERT INTO {table}
                        (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, radius, photo_path)
                        VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, rad_f, photo_v),
                    )
                )
                attempts.append(
                    (
                        f"""
                        INSERT INTO {table}
                        (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, radius, photo_path)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, rad_f, photo_v),
                    )
                )
            else:
                attempts.append(
                    (
                        f"""
                        INSERT INTO {table}
                        (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, radius, photo_path)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, rad_f, photo_v),
                    )
                )
                attempts.append(
                    (
                        f"""
                        INSERT INTO {table}
                        (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, radius, photo_path)
                        VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, rad_f, photo_v),
                    )
                )

        # radius (tanpa photo_path) - behavior lama
        if prefer_jsonb_cast:
            attempts.append(
                (
                    f"""
                    INSERT INTO {table}
                    (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, radius)
                    VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s)
                    """,
                    (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, rad_f),
                )
            )
            attempts.append(
                (
                    f"""
                    INSERT INTO {table}
                    (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, radius)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, rad_f),
                )
            )
        else:
            attempts.append(
                (
                    f"""
                    INSERT INTO {table}
                    (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, radius)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, rad_f),
                )
            )
            attempts.append(
                (
                    f"""
                    INSERT INTO {table}
                    (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, radius)
                    VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s)
                    """,
                    (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, rad_f),
                )
            )
    else:
        # no radius + photo_path
        if photo_v is not None:
            if prefer_jsonb_cast:
                attempts.append(
                    (
                        f"""
                        INSERT INTO {table}
                        (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, photo_path)
                        VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s)
                        """,
                        (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, photo_v),
                    )
                )
                attempts.append(
                    (
                        f"""
                        INSERT INTO {table}
                        (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, photo_path)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, photo_v),
                    )
                )
            else:
                attempts.append(
                    (
                        f"""
                        INSERT INTO {table}
                        (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, photo_path)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, photo_v),
                    )
                )
                attempts.append(
                    (
                        f"""
                        INSERT INTO {table}
                        (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan, photo_path)
                        VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s)
                        """,
                        (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan, photo_v),
                    )
                )

        # no radius (tanpa photo_path) - behavior lama
        if prefer_jsonb_cast:
            attempts.append(
                (
                    f"""
                    INSERT INTO {table}
                    (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan)
                    VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)
                    """,
                    (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan),
                )
            )
            attempts.append(
                (
                    f"""
                    INSERT INTO {table}
                    (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan),
                )
            )
        else:
            attempts.append(
                (
                    f"""
                    INSERT INTO {table}
                    (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan),
                )
            )
            attempts.append(
                (
                    f"""
                    INSERT INTO {table}
                    (waktu, id_relawan, kode_posko, jawaban, skor, status, latitude, longitude, catatan)
                    VALUES (%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)
                    """,
                    (w, id_relawan, kode_posko, payload, float(skor), status, lat_f, lon_f, catatan),
                )
            )


    last_err: Optional[Exception] = None
    for sql, params in attempts:
        try:
            pg_execute(sql, params)
            return True
        except Exception as e:
            last_err = e
            continue

    if last_err:
        raise last_err
    return False


def pg_insert_asesmen_kesehatan(
    id_relawan: str,
    kode_posko: Optional[str],
    jawaban: Dict[str, Any],
    skor: float,
    status: str,
    latitude: Any,
    longitude: Any,
    catatan: Optional[str] = None,
    photo_path: Optional[str] = None,
    radius: Optional[float] = None,
    waktu: Optional[datetime] = None,
) -> bool:
    return _insert_asesmen(
        table_env="PG_ASESMEN_KESEHATAN_TABLE",
        default_table="public.asesmen_kesehatan",
        id_relawan=id_relawan,
        kode_posko=kode_posko,
        jawaban=jawaban,
        skor=skor,
        status=status,
        latitude=latitude,
        longitude=longitude,
        catatan=catatan,
        photo_path=photo_path,
        radius=radius,
        waktu=waktu,
        prefer_jsonb_cast=True,
    )

def pg_insert_asesmen_pendidikan(
    id_relawan: str,
    kode_posko: Optional[str],
    jawaban: Dict[str, Any],
    skor: float,
    status: str,
    latitude: Any,
    longitude: Any,
    catatan: Optional[str] = None,
    photo_path: Optional[str] = None,
    radius: Optional[float] = None,
    waktu: Optional[datetime] = None,
) -> bool:
    return _insert_asesmen(
        table_env="PG_ASESMEN_PENDIDIKAN_TABLE",
        default_table="public.asesmen_pendidikan",
        id_relawan=id_relawan,
        kode_posko=kode_posko,
        jawaban=jawaban,
        skor=skor,
        status=status,
        latitude=latitude,
        longitude=longitude,
        catatan=catatan,
        photo_path=photo_path,
        radius=radius,
        waktu=waktu,
        prefer_jsonb_cast=False,
    )

def pg_insert_asesmen_infrastruktur(
    id_relawan: str,
    kode_posko: Optional[str],
    jawaban: Dict[str, Any],
    skor: float,
    status: str,
    latitude: Any,
    longitude: Any,
    catatan: Optional[str] = None,
    photo_path: Optional[str] = None,
    radius: Optional[float] = None,
    waktu: Optional[datetime] = None,
) -> bool:
    return _insert_asesmen(
        table_env="PG_ASESMEN_INFRASTRUKTUR_TABLE",
        default_table="public.asesmen_infrastruktur",
        id_relawan=id_relawan,
        kode_posko=kode_posko,
        jawaban=jawaban,
        skor=skor,
        status=status,
        latitude=latitude,
        longitude=longitude,
        catatan=catatan,
        photo_path=photo_path,
        radius=radius,
        waktu=waktu,
        prefer_jsonb_cast=False,
    )

def pg_insert_asesmen_psikososial(
    id_relawan: str,
    kode_posko: Optional[str],
    jawaban: Dict[str, Any],
    skor: float,
    status: str,
    latitude: Any,
    longitude: Any,
    catatan: Optional[str] = None,
    photo_path: Optional[str] = None,
    radius: Optional[float] = None,
    waktu: Optional[datetime] = None,
) -> bool:
    return _insert_asesmen(
        table_env="PG_ASESMEN_PSIKOSOSIAL_TABLE",
        default_table="public.asesmen_psikososial",
        id_relawan=id_relawan,
        kode_posko=kode_posko,
        jawaban=jawaban,
        skor=skor,
        status=status,
        latitude=latitude,
        longitude=longitude,
        catatan=catatan,
        photo_path=photo_path,
        radius=radius,
        waktu=waktu,
        prefer_jsonb_cast=False,
    )

def pg_insert_asesmen_wash(
    id_relawan: str,
    kode_posko: Optional[str],
    jawaban: Dict[str, Any],
    skor: float,
    status: str,
    latitude: Any,
    longitude: Any,
    catatan: Optional[str] = None,
    photo_path: Optional[str] = None,
    radius: Optional[float] = None,
    waktu: Optional[datetime] = None,
) -> bool:
    return _insert_asesmen(
        table_env="PG_ASESMEN_WASH_TABLE",
        default_table="public.asesmen_wash",
        id_relawan=id_relawan,
        kode_posko=kode_posko,
        jawaban=jawaban,
        skor=skor,
        status=status,
        latitude=latitude,
        longitude=longitude,
        catatan=catatan,
        photo_path=photo_path,
        radius=radius,
        waktu=waktu,
        prefer_jsonb_cast=False,
    )

def pg_insert_asesmen_kondisi(
    id_relawan: str,
    kode_posko: Optional[str],
    jawaban: Dict[str, Any],
    skor: float,
    status: str,
    latitude: Any,
    longitude: Any,
    catatan: Optional[str] = None,
    photo_path: Optional[str] = None,
    radius: Optional[float] = None,
    waktu: Optional[datetime] = None,
) -> bool:
    return _insert_asesmen(
        table_env="PG_ASESMEN_KONDISI_TABLE",
        default_table="public.asesmen_kondisi",
        id_relawan=id_relawan,
        kode_posko=kode_posko,
        jawaban=jawaban,
        skor=skor,
        status=status,
        latitude=latitude,
        longitude=longitude,
        catatan=catatan,
        photo_path=photo_path,
        radius=radius,
        waktu=waktu,
        prefer_jsonb_cast=False,
    )

# ------------------------------------------------------------------------------
# 7) Lokasi Relawan (marker di peta) - ambil lokasi terakhir per relawan dalam N jam
# ------------------------------------------------------------------------------
def pg_get_relawan_locations_last24h(hours: int = 168) -> List[Dict[str, Any]]:
    """Ambil lokasi relawan terakhir (per relawan) dalam N jam terakhir."""
    relawan_table = _get_env("PG_RELAWAN_TABLE", "public.data_relawan")
    lokasi_table = _get_env("PG_LOKASI_RELAWAN_TABLE", "public.lokasi_relawan")

    sql = f"""
        SELECT DISTINCT ON (lr.id_relawan)
            lr.id_relawan,
            dr.nama_relawan,
            dr.unit,
            dr.photo_path,
            (lr.waktu + INTERVAL '0 hour')::timestamp AS waktu,
            lr.latitude,
            lr.longitude,
            lr.catatan
        FROM {lokasi_table} lr
        LEFT JOIN {relawan_table} dr
          ON dr.id_relawan = lr.id_relawan
        WHERE lr.waktu >= NOW() - (%s * INTERVAL '1 hour')
          AND lr.latitude IS NOT NULL
          AND lr.longitude IS NOT NULL
        ORDER BY lr.id_relawan, lr.waktu DESC;
    """

    rows = pg_fetchall(sql, (hours,))
    out: List[Dict[str, Any]] = []
    for r in rows:
        rr = _json_safe_row(r)
        # Pastikan lat/lon float (hindari Decimal -> error JSON)
        rr["latitude"] = _to_float(rr.get("latitude"))
        rr["longitude"] = _to_float(rr.get("longitude"))


        # radius (opsional)
        rr["radius"] = _to_float(rr.get("radius"))
        out.append(rr)
    return out

def _pg_get_asesmen_last_hours(table_env: str, default_table: str, hours: int =168, only_active: bool = True, start: Optional[date] = None, end: Optional[date] = None) -> List[Dict[str, Any]]:
    """Ambil asesmen dalam N jam terakhir untuk kebutuhan peta (buffer).

    Return field minimal:
      - waktu, id_relawan, skor, status, jawaban, latitude, longitude, catatan
    """
    table = _get_env(table_env, default_table)
    relawan_table = _get_env("PG_RELAWAN_TABLE", "public.data_relawan")
    active_filter = "AND (lr.is_active IS DISTINCT FROM false)" if only_active else ""
    start_filter = "AND lr.waktu::date >= %s" if start else ""
    end_filter = "AND lr.waktu::date <= %s" if end else ""

    # If using date filter, ignore the "last N hours" restriction
    time_limit = ""
    if not start and not end:
        time_limit = "AND lr.waktu >= NOW() - (%s * INTERVAL '1 hour')"
        
    sql = f"""
        SELECT
            lr.id,
            (lr.waktu + INTERVAL '0 hour')::timestamp AS waktu,
            lr.id_relawan,
            dr.nama_relawan,
            lr.kode_posko,
            lr.skor,
            lr.status,
            lr.jawaban,
            lr.latitude,
            lr.longitude,
            lr.catatan,
            lr.radius,
            lr.photo_path,
            lr.is_active
        FROM {table} lr
        LEFT JOIN {relawan_table} dr
          ON dr.id_relawan = lr.id_relawan
        WHERE 1=1
          {time_limit}
          {active_filter}
          {start_filter}
          {end_filter}
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY lr.waktu DESC;
    """

    params = []
    if not start and not end:
        params.append(hours)

    if start:
        params.append(start)
    if end:
        params.append(end)

    try:
        rows = pg_fetchall(sql, tuple(params))
    except Exception:
        # Fallback jika kolom radius belum ada
        sql2 = f"""
            SELECT
                lr.id,
                lr.waktu,
                lr.id_relawan,
                dr.nama_relawan,
                lr.kode_posko,
                lr.skor,
                lr.status,
                lr.jawaban,
                lr.latitude,
                lr.longitude,
                lr.catatan,
                lr.photo_path,
                lr.is_active
            FROM {table} lr
            LEFT JOIN {relawan_table} dr
              ON dr.id_relawan = lr.id_relawan
            WHERE 1=1
              {time_limit}
              {active_filter}
              {start_filter}
              {end_filter}
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY lr.waktu DESC;
        """
        rows = pg_fetchall(sql2, tuple(params))
    out: List[Dict[str, Any]] = []
    for r in rows:
        rr = _json_safe_row(r)

        # Normalisasi lat/lon -> float (hindari Decimal)
        rr["latitude"] = _to_float(rr.get("latitude"))
        rr["longitude"] = _to_float(rr.get("longitude"))

        # Pastikan jawaban selalu dict/JSON-string (untuk front-end)
        j = rr.get("jawaban")
        if isinstance(j, str):
            try:
                rr["jawaban"] = json.loads(j)
            except Exception:
                rr["jawaban"] = j
        out.append(rr)

    return out


def pg_get_asesmen_kesehatan_last24h(hours: int = 168) -> List[Dict[str, Any]]:
    return _pg_get_asesmen_last_hours(
        table_env="PG_ASESMEN_KESEHATAN_TABLE",
        default_table="public.asesmen_kesehatan",
        hours=hours,
    )

def pg_get_asesmen_pendidikan_last24h(hours: int = 168) -> List[Dict[str, Any]]:
    return _pg_get_asesmen_last_hours(
        table_env="PG_ASESMEN_PENDIDIKAN_TABLE",
        default_table="public.asesmen_pendidikan",
        hours=hours,
    )

def pg_get_asesmen_psikososial_last24h(hours: int = 168) -> List[Dict[str, Any]]:
    return _pg_get_asesmen_last_hours(
        table_env="PG_ASESMEN_PSIKOSOSIAL_TABLE",
        default_table="public.asesmen_psikososial",
        hours=hours,
    )

def pg_get_asesmen_infrastruktur_last24h(hours: int = 168) -> List[Dict[str, Any]]:
    return _pg_get_asesmen_last_hours(
        table_env="PG_ASESMEN_INFRASTRUKTUR_TABLE",
        default_table="public.asesmen_infrastruktur",
        hours=hours,
    )

def pg_get_asesmen_wash_last24h(hours: int = 168) -> List[Dict[str, Any]]:
    return _pg_get_asesmen_last_hours(
        table_env="PG_ASESMEN_WASH_TABLE",
        default_table="public.asesmen_wash",
        hours=hours,
    )

def pg_get_asesmen_kondisi_last24h(hours: int = 168) -> List[Dict[str, Any]]:
    return _pg_get_asesmen_last_hours(
        table_env="PG_ASESMEN_KONDISI_TABLE",
        default_table="public.asesmen_kondisi",
        hours=hours,
    )

# ------------------------------------------------------------------------------
# 8) stok_gudang (opsional) - read
# ------------------------------------------------------------------------------
def pg_get_stok_gudang() -> List[Dict[str, Any]]:
    table = _get_env("PG_STOK_GUDANG_TABLE", "public.stok_gudang")
    rows = pg_fetchall(f"SELECT * FROM {table};")
    return [_json_safe_row(r) for r in rows]


# ------------------------------------------------------------------------------
# 9) master_logistik (opsional) - read kode_barang
# ------------------------------------------------------------------------------
def pg_get_master_logistik_codes() -> List[str]:
    table = _get_env("PG_MASTER_LOGISTIK_TABLE", "public.master_logistik")

    # Usahakan ambil dari kolom standar
    sql_candidates = [
        f"SELECT kode_barang FROM {table} WHERE kode_barang IS NOT NULL ORDER BY kode_barang;",
        f"SELECT DISTINCT kode_barang FROM {table} WHERE kode_barang IS NOT NULL ORDER BY kode_barang;",
        f"SELECT DISTINCT kode FROM {table} WHERE kode IS NOT NULL ORDER BY kode;",
    ]

    last_err: Optional[Exception] = None
    for sql in sql_candidates:
        try:
            rows = pg_fetchall(sql)
            out: List[str] = []
            for r in rows:
                v = r.get("kode_barang") if "kode_barang" in r else r.get("kode")
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    out.append(s)
            if out:
                return out
        except Exception as e:
            last_err = e
            continue

    if last_err:
        # Tidak raise supaya app tetap jalan walaupun tabel belum ada
        return []
    return []


# ------------------------------------------------------------------------------
# 10) rekapitulasi kab/kota (opsional) - ambil latest per kab/kota
# ------------------------------------------------------------------------------
def _parse_dt_maybe(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    try:
        s = str(v).strip()
        if not s:
            return None
        # ISO 8601
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def pg_get_rekap_kabkota_latest() -> List[Dict[str, Any]]:
    """Ambil rekap terbaru per kab/kota.

    Karena struktur tabel bisa beda-beda, fungsi ini:
    - SELECT * (biar fleksibel)
    - Grouping di Python, pilih baris terbaru berdasarkan kolom waktu yang tersedia.
    """
    table = _get_env("PG_REKAP_KABKOTA_TABLE", "public.rekapitulasi_data_kabkota")

    try:
        rows = pg_fetchall(f"SELECT * FROM {table};")
    except Exception:
        return []

    # Tentukan kandidat kolom waktu (yang sering dipakai)
    time_keys = ["waktu", "timestamp", "created_at", "updated_at", "tanggal", "date"]

    latest: Dict[str, Dict[str, Any]] = {}
    latest_ts: Dict[str, datetime] = {}

    for r in rows:
        kabkota = r.get("kabkota") or r.get("kabupaten_kota") or r.get("nama_kabkota")
        if not kabkota:
            continue
        kabkota_key = str(kabkota).strip()

        ts: Optional[datetime] = None
        for k in time_keys:
            if k in r:
                ts = _parse_dt_maybe(r.get(k))
                if ts:
                    break
        if ts is None:
            # kalau tidak ada kolom waktu, treat sebagai paling lama
            ts = datetime(1970, 1, 1)

        prev = latest_ts.get(kabkota_key)
        if prev is None or ts >= prev:
            latest_ts[kabkota_key] = ts
            latest[kabkota_key] = r

    return [_json_safe_row(v) for v in latest.values()]


# ------------------------------------------------------------------------------
# 11) permintaan_posko (opsional) - insert
# ------------------------------------------------------------------------------
def pg_next_id(
    table_env: str,
    default_table: str,
    id_col: str,
    prefix: str,
    width: int = 4,
) -> str:
    """Generate ID seperti style lama: R0001, R0002, dst.

    Kalau tidak bisa query max, fallback ke timestamp (MMDDHHMM).
    """
    table = _get_env(table_env, default_table)
    like = f"{prefix}%"

    try:
        sql = f"""
            SELECT {id_col} AS last_id
            FROM {table}
            WHERE {id_col} LIKE %s
            ORDER BY {id_col} DESC
            LIMIT 1;
        """
        rows = pg_fetchall(sql, (like,))
        last_id = (rows[0].get("last_id") if rows else None) or ""
        last_id = str(last_id).strip()

        if last_id.startswith(prefix):
            num_part = last_id[len(prefix):]
            n = int(num_part) if num_part.isdigit() else 0
            return f"{prefix}{(n + 1):0{width}d}"

        return f"{prefix}0001"
    except Exception:
        return f"{prefix}{datetime.now().strftime('%m%d%H%M')}"

def pg_insert_permintaan_posko(data: Dict[str, Any]) -> bool:
    """Insert permintaan posko ke Postgres (kalau tabel tersedia)."""
    table = _get_env("PG_PERMINTAAN_POSKO_TABLE", "public.permintaan_posko")

    # Pastikan id_permintaan ada (biar konsisten dengan UI/log)
    id_permintaan = data.get("id_permintaan")
    if not id_permintaan:
        id_permintaan = pg_next_id("PG_PERMINTAAN_POSKO_TABLE", table, "id_permintaan", "R")
        data["id_permintaan"] = id_permintaan

    # Normalisasi field yang sering dipakai UI
    # waktu = data.get("tanggal")
    # if not waktu:
    #     data["tanggal"] = datetime.now().strftime("%d-%B-%y")

    data["tanggal"] = datetime.now(timezone.utc).replace(tzinfo=None)

    # Coba beberapa mapping kolom supaya tahan beda skema
    attempts: List[Tuple[str, Tuple[Any, ...]]] = [
        (
            f"""
            INSERT INTO {table}
            (id_permintaan, waktu, kode_posko, kode_barang, jumlah_diminta, status, keterangan, relawan, photo_link)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                data.get("id_permintaan"),
                data.get("tanggal"),
                data.get("kode_posko"),
                data.get("kode_barang"),
                data.get("jumlah_diminta"),
                data.get("status"),
                data.get("keterangan"),
                data.get("relawan"),
                data.get("photo_link"),
            ),
        ),
        (
            f"""
            INSERT INTO {table}
            (id_permintaan, kode_posko, kode_barang, jumlah_diminta, status, keterangan, relawan)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                data.get("id_permintaan"),
                data.get("kode_posko"),
                data.get("kode_barang"),
                data.get("jumlah_diminta"),
                data.get("status"),
                data.get("keterangan"),
                data.get("relawan"),
            ),
        ),
    ]

    last_err: Optional[Exception] = None
    for sql, params in attempts:
        try:
            pg_execute(sql, params)
            return True
        except Exception as e:
            last_err = e
            continue

    if last_err:
        raise last_err
    return False

# ------------------------------------------------------------------------------
# 12) LOGISTIK - Permintaan (public.logistik_permintaan)
# ------------------------------------------------------------------------------

def pg_insert_logistik_permintaan(data: Dict[str, Any]) -> bool:
    """Insert permintaan logistik ke tabel logistik_permintaan.

    Ekspektasi kolom (sesuai skema yang kamu tunjukkan di DBeaver):
      - id (bigserial)
      - tanggal (timestamptz, default now())
      - kode_posko (varchar)
      - keterangan (varchar)
      - status_permintaan (varchar)
      - id_relawan (varchar)
      - photo_link (varchar)
      - latitude (numeric)
      - longitude (numeric)

    Catatan: kita sengaja biarkan DB yang set default tanggal (now())
    kalau field tanggal tidak dikirim.
    """
    table = _get_env("PG_LOGISTIK_PERMINTAAN_TABLE", "public.logistik_permintaan")

    # Defaults (sesuai kebutuhan UI sekarang)
    if not data.get("status_permintaan"):
        data["status_permintaan"] = "Draft"

    # Lat/Lon dipaksa float agar aman
    lat = _to_float(data.get("latitude"))
    lon = _to_float(data.get("longitude"))

    # Simpan (tanpa 'id' karena bigserial)
    sql = f"""
        INSERT INTO {table}
        (kode_posko, keterangan, status_permintaan, id_relawan, photo_link, latitude, longitude)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    pg_execute(
        sql,
        (
            data.get("kode_posko"),
            data.get("keterangan"),
            data.get("status_permintaan"),
            data.get("id_relawan"),
            data.get("photo_link"),
            lat,
            lon,
        ),
    )
    return True


def pg_get_logistik_permintaan_last24h(hours: int = 168) -> List[Dict[str, Any]]:
    """Ambil permintaan logistik dalam N jam terakhir (default 24 jam)."""
    table = _get_env("PG_LOGISTIK_PERMINTAAN_TABLE", "public.logistik_permintaan")
    relawan_table = _get_env("PG_RELAWAN_TABLE", "public.data_relawan")

    h = 168
    try:
        h = max(1, int(hours))
    except Exception:
        h = 168

    sql = f"""
        SELECT
            lr.id,
            lr.waktu,
            lr.kode_posko,
            lr.keterangan,
            lr.status_permintaan,
            lr.id_relawan,
            dr.nama_relawan,
            lr.photo_link,
            lr.latitude,
            lr.longitude
        FROM {table} lr
        LEFT JOIN {relawan_table} dr
          ON dr.id_relawan = lr.id_relawan
        WHERE waktu >= (now() - interval '{h} hours')
        ORDER BY waktu DESC;
    """

    rows = pg_fetchall(sql)
    return [_json_safe_row(r) for r in rows]


def pg_update_logistik_permintaan_status(
    id_permintaan: int,
    status_new: str,
    actor_id_relawan: Optional[str] = None,
    actor_nama_relawan: Optional[str] = None,
    note: Optional[str] = None,
) -> bool:
    table = _get_env("PG_LOGISTIK_PERMINTAAN_TABLE", "public.logistik_permintaan").strip()

    # 1) ambil status lama (buat payload log)
    row_old = pg_fetchone(
        f"SELECT status_permintaan FROM {table} WHERE id=%s LIMIT 1",
        (id_permintaan,),
    )
    if not row_old:
        return False

    old_status = (row_old.get("status_permintaan") or "").strip()

    # 2) update + pastikan benar-benar ada row yang berubah
    sql = f"UPDATE {table} SET status_permintaan=%s WHERE id=%s RETURNING id;"
    rows = pg_fetchall(sql, (status_new, id_permintaan))
    ok = bool(rows)

    # 3) insert log aksi admin (mirip teknik asesmen & data_lokasi)
    if ok:
        try:
            pg_insert_admin_action_log(
                actor_id_relawan=actor_id_relawan,
                actor_nama_relawan=actor_nama_relawan,
                action="CHANGE_STATUS_LOGISTIK",
                target_kind="permintaan_logistik",
                target_table=table,
                target_id=int(id_permintaan),
                note=note,  # note optional saja
                payload={
                    "kind": "status_logistik",
                    "id": int(id_permintaan),
                    "old": old_status,
                    "new": status_new,
                },
            )
        except Exception as e:
            # update jangan gagal hanya karena logging
            print(f"[admin_action_log] insert failed for logistik_permintaan#{id_permintaan}: {e}")

    return ok




# ------------------------------------------------------------------------------
# 13) ADMIN - Soft delete asesmen (is_active=false) + log aksi admin
# ------------------------------------------------------------------------------

def _ensure_admin_action_log_table() -> None:
    """Pastikan tabel log aksi admin tersedia.

    Catatan:
    - Sengaja dibuat ringan & aman: CREATE TABLE IF NOT EXISTS.
    - Jika user DB tidak punya permission CREATE, fungsi ini tidak akan mematikan app
      (aksi log akan di-skip).

    Update:
    - Tambahkan kolom target_ref (text) untuk kebutuhan ID non-integer
      (contoh: data_lokasi pakai id_lokasi varchar).
    """
    table = _get_env("PG_ADMIN_ACTION_LOG_TABLE", "public.admin_action_log")

    sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id bigserial PRIMARY KEY,
            waktu timestamptz NOT NULL DEFAULT now(),
            actor_id_relawan varchar(20),
            actor_nama_relawan text,
            action text NOT NULL,
            target_kind text,
            target_table text,
            target_id bigint,
            note text,
            payload text
        );
    """

    try:
        pg_execute(sql)
    except Exception:
        # Jangan raise: logging sifatnya opsional, app harus tetap jalan
        return

    # Kolom tambahan (aman: IF NOT EXISTS)
    try:
        pg_execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS target_ref text;")
    except Exception:
        # Tidak fatal
        return


def pg_insert_admin_action_log(
    actor_id_relawan: Optional[str],
    actor_nama_relawan: Optional[str],
    action: str,
    target_kind: Optional[str] = None,
    target_table: Optional[str] = None,
    target_id: Optional[int] = None,
    target_ref: Optional[str] = None,
    note: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> bool:
    """Tulis log aksi admin ke tabel admin_action_log.

    - target_id: untuk target integer (mis. asesmen.id)
    - target_ref: untuk target non-integer (mis. data_lokasi.id_lokasi)
    """
    _ensure_admin_action_log_table()

    table = _get_env("PG_ADMIN_ACTION_LOG_TABLE", "public.admin_action_log")
    payload_s = None
    if payload is not None:
        try:
            payload_s = json.dumps(payload, ensure_ascii=False)
        except Exception:
            payload_s = str(payload)

    # Coba insert dengan target_ref dulu (jika kolom belum ada, akan fallback)
    sql_new = f"""
        INSERT INTO {table}
        (actor_id_relawan, actor_nama_relawan, action, target_kind, target_table, target_id, target_ref, note, payload)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    sql_old = f"""
        INSERT INTO {table}
        (actor_id_relawan, actor_nama_relawan, action, target_kind, target_table, target_id, note, payload)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    try:
        pg_execute(
            sql_new,
            (
                actor_id_relawan,
                actor_nama_relawan,
                action,
                target_kind,
                target_table,
                int(target_id) if target_id is not None else None,
                (str(target_ref).strip() if target_ref is not None else None),
                note,
                payload_s,
            ),
        )
        return True
    except Exception:
        try:
            pg_execute(
                sql_old,
                (
                    actor_id_relawan,
                    actor_nama_relawan,
                    action,
                    target_kind,
                    target_table,
                    int(target_id) if target_id is not None else None,
                    note,
                    payload_s,
                ),
            )
            return True
        except Exception:
            return False


def pg_get_admin_action_logs(limit: int = 200) -> List[Dict[str, Any]]:
    """Ambil log aksi admin terbaru."""
    table = _get_env("PG_ADMIN_ACTION_LOG_TABLE", "public.admin_action_log")
    try:
        lim = max(1, min(1000, int(limit)))
    except Exception:
        lim = 200

    sql_new = f"""
        SELECT
            id,
            waktu,
            actor_id_relawan,
            actor_nama_relawan,
            action,
            target_kind,
            target_table,
            target_id,
            target_ref,
            note,
            payload
        FROM {table}
        ORDER BY waktu DESC
        LIMIT {lim};
    """

    sql_old = f"""
        SELECT
            id,
            waktu,
            actor_id_relawan,
            actor_nama_relawan,
            action,
            target_kind,
            target_table,
            target_id,
            note,
            payload
        FROM {table}
        ORDER BY waktu DESC
        LIMIT {lim};
    """

    try:
        try:
            rows = pg_fetchall(sql_new)
        except Exception:
            rows = pg_fetchall(sql_old)
        return [_json_safe_row(r) for r in rows]
    except Exception:
        return []


def pg_set_asesmen_active(

    kind: str,
    asesmen_id: int,
    is_active: bool,
    actor_id_relawan: Optional[str] = None,
    actor_nama_relawan: Optional[str] = None,
    note: Optional[str] = None,
) -> bool:
    """Set is_active True/False untuk 1 record asesmen.

    kind: kesehatan|pendidikan|psikososial|infrastruktur|wash|kondisi
    """
    kind_key = (kind or "").strip().lower()

    kind_map = {
        "kesehatan": ("PG_ASESMEN_KESEHATAN_TABLE", "public.asesmen_kesehatan"),
        "pendidikan": ("PG_ASESMEN_PENDIDIKAN_TABLE", "public.asesmen_pendidikan"),
        "psikososial": ("PG_ASESMEN_PSIKOSOSIAL_TABLE", "public.asesmen_psikososial"),
        "infrastruktur": ("PG_ASESMEN_INFRASTRUKTUR_TABLE", "public.asesmen_infrastruktur"),
        "wash": ("PG_ASESMEN_WASH_TABLE", "public.asesmen_wash"),
        "kondisi": ("PG_ASESMEN_KONDISI_TABLE", "public.asesmen_kondisi"),
    }

    if kind_key not in kind_map:
        raise ValueError("kind asesmen tidak dikenal")

    try:
        aid = int(asesmen_id)
    except Exception as e:
        raise ValueError("ID asesmen tidak valid") from e

    table_env, default_table = kind_map[kind_key]
    table = _get_env(table_env, default_table)

    sql = f"""
        UPDATE {table}
        SET is_active = %s
        WHERE id = %s
        RETURNING id;
    """

    rows = pg_fetchall(sql, (bool(is_active), aid))
    ok = bool(rows)

    if ok:
        action = "ACTIVATE_ASESMEN" if bool(is_active) else "DEACTIVATE_ASESMEN"
        pg_insert_admin_action_log(
            actor_id_relawan=actor_id_relawan,
            actor_nama_relawan=actor_nama_relawan,
            action=action,
            target_kind=kind_key,
            target_table=table,
            target_id=aid,
            note=note,
            payload={"kind": kind_key, "id": aid, "is_active": bool(is_active)},
        )

    return ok


def pg_deactivate_asesmen(
    kind: str,
    asesmen_id: int,
    actor_id_relawan: Optional[str] = None,
    actor_nama_relawan: Optional[str] = None,
    note: Optional[str] = None,
) -> bool:
    """Backward compatible: set is_active=false."""
    return pg_set_asesmen_active(
        kind=kind,
        asesmen_id=asesmen_id,
        is_active=False,
        actor_id_relawan=actor_id_relawan,
        actor_nama_relawan=actor_nama_relawan,
        note=note,
    )


# ------------------------------------------------------------------------------
# 13b) ADMIN - data_lokasi: is_active (soft delete) + ubah jenis_lokasi + log
# ------------------------------------------------------------------------------

def _ensure_data_lokasi_is_active_column() -> None:
    """Pastikan kolom is_active ada di tabel data_lokasi.

    Dibuat aman: ALTER TABLE IF NOT EXISTS. Jika tidak ada permission, fungsi ini tidak mematikan app.
    """
    table = _get_env("PG_DATA_LOKASI_TABLE", "public.data_lokasi")
    try:
        pg_execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT TRUE;")
    except Exception:
        return


def pg_get_admin_lokasi_list(
    limit: int = 10, 
    offset: int = 0, 
    search: str = "", 
    kind: str = "", 
    start: Optional[Union[str, date]] = None, 
    end: Optional[Union[str, date]] = None
) -> Dict[str, Any]:
    """Ambil daftar data_lokasi (aktif + nonaktif) untuk panel admin dengan filter dan pagination."""
    _ensure_data_lokasi_is_active_column()

    table = _get_env("PG_DATA_LOKASI_TABLE", "public.data_lokasi")
    
    try:
        lim = max(1, min(100, int(limit)))
        off = max(0, int(offset))
    except Exception:
        lim = 10
        off = 0

    where_clauses = ["1=1"]
    params = []

    if search.strip():
        where_clauses.append("(nama_lokasi ILIKE %s OR nama_kabkota ILIKE %s OR id_lokasi ILIKE %s)")
        s = f"%{search.strip()}%"
        params.extend([s, s, s])

    if kind.strip():
        where_clauses.append("jenis_lokasi = %s")
        params.append(kind.strip())

    if start:
        where_clauses.append("waktu::date >= %s")
        params.append(start)
    if end:
        where_clauses.append("waktu::date <= %s")
        params.append(end)

    where_sql = " AND ".join(where_clauses)

    sql = f"""
        SELECT
            id_lokasi,
            jenis_lokasi,
            nama_kabkota,
            nama_lokasi,
            waktu,
            COALESCE(is_active, TRUE) AS is_active
        FROM {table}
        WHERE {where_sql}
        ORDER BY waktu DESC
        LIMIT %s OFFSET %s;
    """
    
    # Query for has_more
    sql_count = f"SELECT COUNT(*) as total FROM {table} WHERE {where_sql};"

    try:
        rows = pg_fetchall(sql, (*params, lim + 1, off))
        has_more = len(rows) > lim
        paged_rows = rows[:lim]

        return {
            "rows": [_json_safe_row(r) for r in paged_rows],
            "has_more": has_more
        }
    except Exception:
        return {"rows": [], "has_more": False}


def pg_set_data_lokasi_active(
    id_lokasi: str,
    is_active: bool,
    actor_id_relawan: Optional[str] = None,
    actor_nama_relawan: Optional[str] = None,
    note: Optional[str] = None,
) -> bool:
    """Set is_active True/False untuk 1 record data_lokasi."""
    _ensure_data_lokasi_is_active_column()

    table = _get_env("PG_DATA_LOKASI_TABLE", "public.data_lokasi")
    sid = str(id_lokasi or '').strip()
    if not sid:
        raise ValueError('ID lokasi tidak valid')

    sql = f"""
        UPDATE {table}
        SET is_active = %s
        WHERE id_lokasi = %s
        RETURNING id_lokasi;
    """

    rows = pg_fetchall(sql, (bool(is_active), sid))
    ok = bool(rows)

    if ok:
        action = "ACTIVATE_LOKASI" if bool(is_active) else "DEACTIVATE_LOKASI"
        pg_insert_admin_action_log(
            actor_id_relawan=actor_id_relawan,
            actor_nama_relawan=actor_nama_relawan,
            action=action,
            target_kind="data_lokasi",
            target_table=table,
            target_id=None,
            target_ref=sid,
            note=note,
            payload={"id_lokasi": sid, "is_active": bool(is_active)},
        )

    return ok


def pg_update_data_lokasi_jenis(
    id_lokasi: str,
    jenis_lokasi: str,
    actor_id_relawan: Optional[str] = None,
    actor_nama_relawan: Optional[str] = None,
    note: Optional[str] = None,
) -> bool:
    """Ubah jenis_lokasi pada data_lokasi (nilai diambil dari ref_jenis_lokasi)."""
    table = _get_env("PG_DATA_LOKASI_TABLE", "public.data_lokasi")
    sid = str(id_lokasi or '').strip()
    new_jenis = str(jenis_lokasi or '').strip()
    if not sid:
        raise ValueError('ID lokasi tidak valid')
    if not new_jenis:
        raise ValueError('Jenis lokasi tidak valid')

    # Validasi ringan: pastikan new_jenis ada di daftar ref_jenis_lokasi jika tabelnya tersedia
    try:
        allowed = set([v.strip() for v in (pg_get_ref_jenis_lokasi() or []) if str(v).strip()])
    except Exception:
        allowed = set()

    if allowed and (new_jenis not in allowed):
        raise ValueError('Jenis lokasi tidak ada di ref_jenis_lokasi')

    # Ambil nilai lama (untuk log)
    old_jenis = None
    try:
        r0 = pg_fetchone(f"SELECT jenis_lokasi FROM {table} WHERE id_lokasi = %s", (sid,))
        if r0:
            old_jenis = r0.get('jenis_lokasi')
    except Exception:
        old_jenis = None

    sql = f"""
        UPDATE {table}
        SET jenis_lokasi = %s
        WHERE id_lokasi = %s
        RETURNING id_lokasi;
    """

    rows = pg_fetchall(sql, (new_jenis, sid))
    ok = bool(rows)

    if ok:
        pg_insert_admin_action_log(
            actor_id_relawan=actor_id_relawan,
            actor_nama_relawan=actor_nama_relawan,
            action="UPDATE_JENIS_LOKASI",
            target_kind="data_lokasi",
            target_table=table,
            target_id=None,
            target_ref=sid,
            note=note,
            payload={"id_lokasi": sid, "old": old_jenis, "new": new_jenis},
        )

    return ok

def pg_get_admin_asesmen_list(hours: int = 24, limit_per_kind: int = 500, start: Optional[date] = None, end: Optional[date] = None, kind_filter: str = "", offset: int = 0, limit: int = 10) -> Dict[str, Any]:
    """Ambil daftar asesmen (aktif + nonaktif) untuk panel admin dengan pagination.

    Return: {"rows": [...], "has_more": bool}
    """
    try:
        h = int(hours)
    except Exception:
        h = 24
    h = max(1, min(24 * 60, h))

    try:
        lim_k = int(limit_per_kind)
    except Exception:
        lim_k = 500
    lim_k = max(1, min(2000, lim_k))

    try:
        off = int(offset)
    except Exception:
        off = 0
    
    try:
        lim = int(limit)
    except Exception:
        lim = 10

    if isinstance(start, str) and start:
        try:
            start = datetime.strptime(start, "%Y-%m-%d").date()
        except Exception:
            start = None

    if isinstance(end, str) and end:
        try:
            end = datetime.strptime(end, "%Y-%m-%d").date()
        except Exception:
            end = None

    buckets = [
        ("kesehatan", "PG_ASESMEN_KESEHATAN_TABLE", "public.asesmen_kesehatan"),
        ("pendidikan", "PG_ASESMEN_PENDIDIKAN_TABLE", "public.asesmen_pendidikan"),
        ("psikososial", "PG_ASESMEN_PSIKOSOSIAL_TABLE", "public.asesmen_psikososial"),
        ("infrastruktur", "PG_ASESMEN_INFRASTRUKTUR_TABLE", "public.asesmen_infrastruktur"),
        ("wash", "PG_ASESMEN_WASH_TABLE", "public.asesmen_wash"),
        ("kondisi", "PG_ASESMEN_KONDISI_TABLE", "public.asesmen_kondisi"),
    ]

    all_data: List[Dict[str, Any]] = []
    for kind, env, default_table in buckets:
        # Jika ada filter jenis, skip yang tidak cocok
        if kind_filter and kind_filter.strip().lower() != kind:
            continue

        try:
            # Kita ambil lim_k (cukup banyak) dari tiap tabel untuk disortir gabungan
            rows = _pg_get_asesmen_last_hours(env, default_table, hours=h, only_active=False, start=start, end=end)
        except Exception:
            rows = []

        for r in (rows or [])[:lim_k]:
            rr = _json_safe_row(r)
            all_data.append(
                {
                    "kind": kind,
                    "id": rr.get("id"),
                    "kode_posko": rr.get("kode_posko"),
                    "id_relawan": rr.get("id_relawan"),
                    "nama_relawan": rr.get("nama_relawan"),
                    "waktu": rr.get("waktu"),
                    "skor": rr.get("skor"),
                    "status": rr.get("status"),
                    "is_active": rr.get("is_active"),
                }
            )

    # Sort gabungan by waktu DESC
    all_data.sort(key=lambda x: str(x.get("waktu") or ""), reverse=True)
    
    # Slicing untuk pagination
    paged_rows = all_data[off : off + lim]
    has_more = len(all_data) > (off + lim)

    return {"rows": paged_rows, "has_more": has_more, "total_all_loaded": len(all_data)}

