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
    """Ambil data lokasi (posko/lokasi lain) dari Postgres.

    Return list-of-dict kompatibel dengan map.html:
    - kode_lokasi (fallback untuk id_lokasi)
    - id_lokasi
    - jenis_lokasi
    - kabupaten_kota (fallback untuk nama_kabkota)
    - nama_kabkota
    - nama_lokasi
    - status_lokasi
    - tingkat_akses
    - kondisi
    - catatan
    - photo_path
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

        id_lokasi = (
            r.get("id_lokasi")
            or r.get("kode_lokasi")
            or r.get("kode")
            or r.get("kode_posko")
            or ""
        )
        jenis = r.get("jenis_lokasi") or r.get("jenis") or r.get("tipe") or ""

        nama_kabkota = (
            r.get("nama_kabkota")
            or r.get("kabupaten_kota")
            or r.get("kabkota")
            or r.get("wilayah")
            or ""
        )

        nama_lokasi = r.get("nama_lokasi") or r.get("nama") or nama_kabkota or id_lokasi

        status_lokasi = r.get("status_lokasi") or r.get("status") or ""
        tingkat_akses = r.get("tingkat_akses") or r.get("akses") or r.get("aksesibilitas") or ""
        kondisi = r.get("kondisi") or r.get("kondisi_umum") or ""

        catatan = (
            r.get("catatan")
            or r.get("keterangan")
            or r.get("lokasi_text")
            or ""
        )

        photo_path = r.get("photo_path") or r.get("photo") or r.get("photo_link") or ""

        out.append(
            {
                # Tetap ada untuk kompatibilitas UI lama
                "kode_lokasi": str(id_lokasi) if id_lokasi is not None else "",
                "kabupaten_kota": str(nama_kabkota) if nama_kabkota is not None else "",
                "kondisi_umum": str(kondisi) if kondisi is not None else "",
                "akses": str(tingkat_akses) if tingkat_akses is not None else "",

                # Field tambahan sesuai struktur DB terbaru
                "id_lokasi": str(id_lokasi) if id_lokasi is not None else "",
                "jenis_lokasi": str(jenis) if jenis is not None else "",
                "nama_kabkota": str(nama_kabkota) if nama_kabkota is not None else "",
                "nama_lokasi": str(nama_lokasi) if nama_lokasi is not None else "",
                "status_lokasi": str(status_lokasi) if status_lokasi is not None else "",
                "tingkat_akses": str(tingkat_akses) if tingkat_akses is not None else "",
                "kondisi": str(kondisi) if kondisi is not None else "",
                "catatan": str(catatan) if catatan is not None else "",
                "photo_path": str(photo_path) if photo_path is not None else "",
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
    if rad_f is not None:
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
        rr["latitude"] = _to_float(rr.get("latitude"))
        rr["longitude"] = _to_float(rr.get("longitude"))


        # radius (opsional)
        rr["radius"] = _to_float(rr.get("radius"))
        out.append(rr)
    return out

def _pg_get_asesmen_last_hours(table_env: str, default_table: str, hours: int =168) -> List[Dict[str, Any]]:
    """Ambil asesmen dalam N jam terakhir untuk kebutuhan peta (buffer).

    Return field minimal:
      - waktu, id_relawan, skor, status, jawaban, latitude, longitude, catatan
    """
    table = _get_env(table_env, default_table)
    relawan_table = _get_env("PG_RELAWAN_TABLE", "public.data_relawan")

    sql = f"""
        SELECT
            lr.waktu,
            lr.id_relawan,
            dr.nama_relawan,
            lr.skor,
            lr.status,
            lr.jawaban,
            lr.latitude,
            lr.longitude,
            lr.catatan,
            lr.radius
        FROM {table} lr
        LEFT JOIN {relawan_table} dr
          ON dr.id_relawan = lr.id_relawan
        WHERE waktu >= NOW() - (%s * INTERVAL '1 hour')
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY waktu DESC;
    """

    try:
        rows = pg_fetchall(sql, (hours,))
    except Exception:
        # Fallback jika kolom radius belum ada
        sql2 = f"""
            SELECT
                lr.waktu,
                lr.id_relawan,
                dr.nama_relawan,
                lr.skor,
                lr.status,
                lr.jawaban,
                lr.latitude,
                lr.longitude,
                lr.catatan,
            FROM {table} lr
            LEFT JOIN {relawan_table} dr
              ON dr.id_relawan = lr.id_relawan
            WHERE waktu >= NOW() - (%s * INTERVAL '1 hour')
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY waktu DESC;
        """
        rows = pg_fetchall(sql2, (hours,))
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


def pg_update_logistik_permintaan_status(id_permintaan: int, status_permintaan: str) -> bool:
    """Update kolom status_permintaan pada tabel logistik_permintaan berdasarkan ID.

    Status yang didukung (sesuai kebutuhan UI):
      Draft, Diproses, Dikirim, Diterima, Ditolak

    Catatan:
    - id_permintaan mengacu ke kolom 'id' (bigserial).
    - Validasi status dilakukan di sini agar backend aman (tidak sembarang teks).
    """
    table = _get_env("PG_LOGISTIK_PERMINTAAN_TABLE", "public.logistik_permintaan")

    allowed = {"Draft", "Diproses", "Dikirim", "Diterima", "Ditolak"}
    st = (status_permintaan or "").strip()

    # Normalisasi sederhana: Title Case untuk aman
    # (agar input "draft" tetap bisa diterima)
    if st.lower() == "draft":
        st = "Draft"
    elif st.lower() == "diproses":
        st = "Diproses"
    elif st.lower() == "dikirim":
        st = "Dikirim"
    elif st.lower() == "diterima":
        st = "Diterima"
    elif st.lower() == "ditolak":
        st = "Ditolak"

    if st not in allowed:
        raise ValueError("Status_permintaan tidak valid")

    try:
        pid = int(id_permintaan)
    except Exception as e:
        raise ValueError("ID permintaan tidak valid") from e

    sql = f"""
        UPDATE {table}
        SET status_permintaan = %s
        WHERE id = %s
    """

    pg_execute(sql, (st, pid))
    return True
