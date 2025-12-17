"""pg_data.py

Helper untuk baca/tulis data dari PostgreSQL (SATGAS) TANPA merusak code lama yang masih pakai Google Sheets.

Dipakai untuk:
1) status kab/kota (public.data_status_kabkota) -> status_map untuk fill polygon
2) GeoJSON polygon kab/kota (public.geo_kabkota) -> ditulis ke static/data/kabkota_sumut.json
3) data_lokasi (public.data_lokasi) -> marker posko/lokasi
4) data_relawan (public.data_relawan) -> login
5) lokasi_relawan (public.lokasi_relawan) -> submit absensi lokasi
6) asesmen_kesehatan (public.asesmen_kesehatan)
7) asesmen_pendidikan (public.asesmen_pendidikan)

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
  - GEOJSON_TTL_SECONDS          default: 86400 (1 hari)
  - FORCE_GEOJSON_REFRESH        default: 0

Catatan:
- Tetap kompatibel dengan psycopg v3 (psycopg) maupun psycopg2.
- Untuk awal, tiap call query buka-konek-tutup (simpel & aman).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
    """Ambil data lokasi (posko/lokasi lain) dari Postgres.

    Return list-of-dict yang kompatibel dengan map.html:
    - kode_lokasi
    - jenis_lokasi
    - kabupaten_kota
    - nama_lokasi
    - kondisi_umum
    - akses
    - latitude, longitude
    """
    table = _get_env("PG_DATA_LOKASI_TABLE", "public.data_lokasi")
    rows = pg_fetchall(f"SELECT * FROM {table};")

    out: List[Dict[str, Any]] = []
    for r in rows:
        lat = _to_float(r.get("latitude") if "latitude" in r else r.get("lat"))
        lon = _to_float(
            r.get("longitude")
            if "longitude" in r
            else (r.get("lon") if "lon" in r else r.get("lng"))
        )
        kode = r.get("kode_lokasi") or r.get("kode") or r.get("id_lokasi") or r.get("kode_posko") or ""
        jenis = r.get("jenis_lokasi") or r.get("jenis") or r.get("tipe") or ""
        kabkota = (
            r.get("kabupaten_kota")
            or r.get("kabkota")
            or r.get("nama_kabkota")
            or r.get("wilayah")
            or ""
        )
        nama = r.get("nama_lokasi") or r.get("nama") or kabkota or kode

        out.append(
            {
                "kode_lokasi": str(kode) if kode is not None else "",
                "jenis_lokasi": str(jenis) if jenis is not None else "",
                "kabupaten_kota": str(kabkota) if kabkota is not None else "",
                "nama_lokasi": str(nama) if nama is not None else "",
                "kondisi_umum": (r.get("kondisi_umum") or r.get("kondisi") or "") if r is not None else "",
                "akses": (r.get("akses") or r.get("aksesibilitas") or "") if r is not None else "",
                "latitude": lat,
                "longitude": lon,
            }
        )

    return out


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
            kode_akses
        FROM {table}
        WHERE nama_relawan IS NOT NULL;
    """
    rows = pg_fetchall(sql)
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id_relawan": r.get("id_relawan"),
                "nama_relawan": r.get("nama_relawan"),
                "kode_akses": r.get("kode_akses"),
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
    waktu: Optional[datetime] = None,
    prefer_jsonb_cast: bool = True,
) -> bool:
    table = _get_env(table_env, default_table)

    lat_f = _to_float(latitude)
    lon_f = _to_float(longitude)
    w = waktu or datetime.now(timezone.utc).replace(tzinfo=None)

    payload = json.dumps(jawaban, ensure_ascii=False)

    # Ada DB yang kolom jawaban-nya TEXT, ada yang jsonb. Kita coba 2 cara.
    attempts: List[Tuple[str, Tuple[Any, ...]]] = []

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
        waktu=waktu,
        prefer_jsonb_cast=False,
    )

# ------------------------------------------------------------------------------
# 3) Lokasi Relawan (marker di peta) - ambil lokasi terakhir per relawan dalam 24 jam
# ------------------------------------------------------------------------------
import datetime as _dt
from decimal import Decimal as _Decimal

def _json_safe_value(v: Any) -> Any:
    """Konversi tipe yang sering muncul dari Postgres agar aman untuk JSON."""
    if v is None:
        return None
    if isinstance(v, _Decimal):
        return float(v)
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.isoformat()
    return v

def _json_safe_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _json_safe_value(v) for k, v in row.items()}

def pg_get_relawan_locations_last24h(hours: int = 24) -> List[Dict[str, Any]]:
    """Ambil lokasi relawan terakhir (per relawan) dalam N jam terakhir."""
    relawan_table = _get_env("PG_RELAWAN_TABLE", "public.data_relawan")
    lokasi_table  = _get_env("PG_LOKASI_RELAWAN_TABLE", "public.lokasi_relawan")

    sql = f"""
        SELECT DISTINCT ON (lr.id_relawan)
            lr.id_relawan,
            dr.nama_relawan,
            lr.waktu,
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
        try:
            rr["latitude"] = float(rr["latitude"]) if rr.get("latitude") is not None else None
            rr["longitude"] = float(rr["longitude"]) if rr.get("longitude") is not None else None
        except Exception:
            pass
        out.append(rr)
    return out
