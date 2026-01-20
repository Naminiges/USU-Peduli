from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, send_from_directory, abort
import json
import datetime
import os  # Untuk mendapatkan waktu saat ini dan Secret Key
import math
import time
import gspread
from itertools import groupby
from pathlib import Path
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path
from dotenv import load_dotenv
from media_upload import save_asesmen_photos, photos_to_photo_path_value, save_lokasi_photo
from zoneinfo import ZoneInfo

CACHE_STOK = {"data": [], "timestamp": 0}
CACHE_REKAP = {"data": [], "timestamp": 0}
CACHE_DISTRIBUSI = {"data": [], "timestamp": 0} 
load_dotenv()

# def dd(data):
#     """
#     Fungsi Dump and Die ala Laravel untuk Flask.
#     Mencetak data ke terminal dan menampilkan JSON di browser, lalu stop.
#     """
#     print("\n" + "="*30)
#     print(" DUMPING DATA ")
#     print("="*30)
#     print(data) # Cetak di terminal
#     print("="*30 + "\n")
    
#     # Paksa berhenti dan tampilkan data sebagai JSON di browser
#     response = jsonify(data)
#     response.status_code = 200
#     abort(response)

# ------------------------------------------------------------------------------
# PostgreSQL helper (utama)
# ------------------------------------------------------------------------------
try:
    from pg_data import (
        pg_get_status_map,
        pg_get_data_lokasi,
        pg_get_relawan_list,
        pg_insert_lokasi_relawan,
        pg_insert_asesmen_kesehatan,
        pg_insert_asesmen_pendidikan,
        pg_insert_asesmen_infrastruktur,
        pg_insert_asesmen_wash,
        pg_insert_asesmen_kondisi,
        pg_insert_asesmen_psikososial,
        pg_get_relawan_locations_last24h,
        pg_get_asesmen_kesehatan_last24h,
        pg_get_asesmen_pendidikan_last24h,
        pg_get_asesmen_infrastruktur_last24h,
        pg_get_asesmen_wash_last24h,
        pg_get_asesmen_kondisi_last24h,
        pg_get_asesmen_psikososial_last24h,
        ensure_kabkota_geojson_static,
        pg_get_logistik_permintaan_last24h,
        pg_insert_logistik_permintaan,
        pg_update_logistik_permintaan_status,
        pg_deactivate_asesmen,
        pg_set_asesmen_active,
        pg_get_admin_asesmen_list,
        pg_get_admin_action_logs,
        pg_get_admin_lokasi_list,
        pg_set_data_lokasi_active,
        pg_update_data_lokasi_jenis,
        pg_insert_data_lokasi,
        pg_update_data_lokasi_photo_path,
        pg_get_ref_jenis_lokasi,
        pg_get_ref_kabkota,
        pg_get_ref_status_lokasi,
        pg_get_ref_tingkat_akses,
        pg_get_ref_kondisi,
        # opsional (kalau tabel ada)
        pg_get_stok_gudang,
        pg_get_master_logistik_codes,
        pg_get_rekap_kabkota_latest,
        pg_insert_permintaan_posko,
        pg_next_id,
        pg_get_asesmen_rekap_by_kabkota,
    )
except Exception as _pg_err:
    print(f"[PG] Error import pg_data: {_pg_err}")
    pg_get_status_map = None
    pg_get_data_lokasi = None
    pg_get_relawan_list = None
    pg_insert_lokasi_relawan = None
    pg_insert_asesmen_kesehatan = None
    pg_insert_asesmen_pendidikan = None
    pg_insert_asesmen_infrastruktur = None
    pg_insert_asesmen_wash = None
    pg_insert_asesmen_kondisi = None
    pg_insert_asesmen_psikososial = None
    pg_get_relawan_locations_last24h = None
    pg_get_asesmen_kesehatan_last24h = None
    pg_get_asesmen_pendidikan_last24h = None
    pg_get_asesmen_infrastruktur_last24h = None
    pg_get_asesmen_wash_last24h = None
    pg_get_asesmen_kondisi_last24h = None
    pg_get_asesmen_psikososial_last24h = None
    ensure_kabkota_geojson_static = None
    pg_get_stok_gudang = None
    pg_get_master_logistik_codes = None
    pg_get_rekap_kabkota_latest = None
    pg_insert_permintaan_posko = None
    pg_next_id = None
    pg_get_asesmen_rekap_by_kabkota = None
    pg_insert_logistik_permintaan = None
    pg_get_logistik_permintaan_last24h = None
    pg_update_logistik_permintaan_status = None
    pg_deactivate_asesmen = None
    pg_set_asesmen_active = None
    pg_get_admin_asesmen_list = None
    pg_get_admin_action_logs = None
    pg_get_admin_lokasi_list = None
    pg_set_data_lokasi_active = None
    pg_update_data_lokasi_jenis = None
    pg_insert_data_lokasi = None
    pg_update_data_lokasi_photo_path = None
    pg_get_ref_jenis_lokasi = None
    pg_get_ref_kabkota = None
    pg_get_ref_status_lokasi = None
    pg_get_ref_tingkat_akses = None
    pg_get_ref_kondisi = None

app = Flask(__name__)

# ------------------------------------------------------------------------------
# Serve MEDIA (foto relawan) dari folder lokal "media/"
# URL: /media/<path>
# ------------------------------------------------------------------------------
MEDIA_DIR = os.environ.get("MEDIA_DIR", str(Path(app.root_path) / "media"))

@app.route("/media/<path:filename>")
def media_files(filename):
    return send_from_directory(MEDIA_DIR, filename)


# HARUS diganti dengan kunci rahasia yang kuat untuk mengamankan sesi
app.secret_key = os.environ.get("SECRET_KEY")
if not app.secret_key:
    raise RuntimeError("SECRET_KEY belum diset di environment")


def _pg_enabled() -> bool:
    return bool(os.environ.get("DATABASE_URL"))

# --- 1. HELPER: KONVERSI TANGGAL INDONESIA KE ISO ---
BULAN_INDO = {
    'januari': '01', 'februari': '02', 'maret': '03', 'april': '04',
    'mei': '05', 'juni': '06', 'juli': '07', 'agustus': '08',
    'september': '09', 'oktober': '10', 'november': '11', 'desember': '12'
}

def convert_tanggal_indo_ke_iso(tgl_str):
    try:
        if not tgl_str: 
            return ""
        s = str(tgl_str).strip()
        
        parts = s.split('-')
        
        if len(parts) == 3:
            tgl = str(parts[0]).strip()
            bln_txt = str(parts[1]).strip()
            thn = str(parts[2]).strip()
            # ===========================================
            
            bln_kode = BULAN_INDO.get(bln_txt.lower(), '01')
            
            # Cek panjang string tahun (Anti-Error len())
            if len(thn) == 2:
                thn = "20" + thn
            
            return f"{thn}-{bln_kode}-{tgl.zfill(2)}"
            
    except Exception as e:
        print(f"[ERROR TANGGAL] Input: '{tgl_str}' | Error: {e}")
        
    return ""


def get_rekap_from_spreadsheet():
    # Cek cache
    if time.time() - CACHE_REKAP["timestamp"] < 300 and CACHE_REKAP["data"]:
        return CACHE_REKAP["data"]

    try:
        # Setup Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('service_account.json', scope)
        client = gspread.authorize(creds)
        
        # URL dari snippet kamu
        url_sheet = "https://docs.google.com/spreadsheets/d/170n5uyiW3zftwZFV77e_mxd8ythgGpgby6RAuVV47oM/edit?usp=sharing"
        sheet = client.open_by_url(url_sheet).worksheet("rekapitulasi_data_kabkota")
        
        # Ambil data mentah
        raw_data = sheet.get_all_records()
        
        # --- PROSES CLEANING DATA ---
        cleaned_data = []
        for row in raw_data:
            # A. Buat kolom tanggal_iso untuk keperluan filter di HTML
            row['tanggal_iso'] = convert_tanggal_indo_ke_iso(row.get('tanggal', ''))
            
            # B. Pastikan kolom angka benar-benar angka (Integer)
            # Jika kosong/None, set jadi 0 agar tidak error di HTML
            row['korban_meninggal'] = int(row.get('korban_meninggal') or 0)
            row['korban_hilang']    = int(row.get('korban_hilang') or 0)
            row['mengungsi']        = int(row.get('mengungsi') or 0)
            # C. Pastikan text tidak None
            row['sumber_info']      = row.get('sumber_info') or "-"
            row['kabkota']          = row.get('kabkota') or "Wilayah Tidak Diketahui"

            cleaned_data.append(row)
        # ----------------------------

        # Simpan data BERSIH ke cache
        CACHE_REKAP["data"] = cleaned_data
        CACHE_REKAP["timestamp"] = time.time()
        
        print("[GSPREAD] Rekap: Berhasil ambil data baru")
        return cleaned_data

    except Exception as e:
        print(f"[GSPREAD] Error mengambil rekap: {e}")
        # Kembalikan cache lama jika ada error koneksi
        return CACHE_REKAP["data"] if CACHE_REKAP["data"] else []

def get_logistik_keluar_grouped():
    # Cek cache khusus distribusi
    if time.time() - CACHE_DISTRIBUSI["timestamp"] < 300 and CACHE_DISTRIBUSI["data"]:
        return CACHE_DISTRIBUSI["data"]

    try:
        # Setup Auth
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name('service_account.json', scope)
        client = gspread.authorize(creds)
        
        # URL Spreadsheet kamu
        url_sheet = "https://docs.google.com/spreadsheets/d/1ZO4m71gw_veXszakUP4SURYdh_sX0I6h4nPjegr73XQ/edit?usp=sharing"
        sheet = client.open_by_url(url_sheet).worksheet("pembersihan_data")
        raw_data = sheet.get_all_records()
        
        grouped_data = {}
        
        for row in raw_data:
            # Ambil key utama (bersihkan spasi)
            tgl = str(row.get('tanggal', '')).strip()
            nama = str(row.get('nama', '')).strip()
            daerah = str(row.get('alamat/daerah', '')).strip()
            
            # Key unik: Gabungan Tanggal + Nama + Daerah
            # Contoh: "6 Dec 2025_Tim Diksaintek_Aceh Tamiang"
            group_key = f"{tgl}_{nama}_{daerah}"
            
            # Jika grup belum ada, buat header-nya
            if group_key not in grouped_data:
                grouped_data[group_key] = {
                    'header': {
                        'tanggal': tgl,
                        'nama': nama,
                        'daerah': daerah
                    },
                    'list_barang': []
                }
            
            # Masukkan barang ke dalam list items
            item_detail = {
                'deskripsi': row.get('deskripsi'),
                'jumlah': row.get('jumlah'),
                'satuan': row.get('satuan'),
                'status': row.get('status_pengiriman')
            }
            grouped_data[group_key]['list_barang'].append(item_detail)
            
        # Ubah ke List agar bisa di-loop di HTML
        final_list = list(grouped_data.values())
        
        # Simpan Cache
        CACHE_DISTRIBUSI["data"] = final_list
        CACHE_DISTRIBUSI["timestamp"] = time.time()
        
        return final_list

    except Exception as e:
        print(f"[GSPREAD] Error distribusi: {e}")
        return []

def ensure_kabkota_geojson_ready():
    """Generate static/data/kabkota_sumut.json dari Postgres bila perlu (tanpa ubah front-end)."""
    if not _pg_enabled() or ensure_kabkota_geojson_static is None:
        return None
    try:
        return ensure_kabkota_geojson_static(app.root_path)
    except Exception as e:
        print(f"[PG] ensure_kabkota_geojson_ready gagal: {e}")
        return None


def get_status_map_any() -> dict:
    """Ambil status kab/kota dari Postgres.
    Output: {KABKOTA_UPPER: status}
    """
    if _pg_enabled() and pg_get_status_map is not None:
        try:
            return pg_get_status_map() or {}
        except Exception as e:
            print(f"[PG] get_status_map_any error: {e}")
    return {}


def get_data_lokasi_any() -> list:
    """Ambil data_lokasi dari Postgres.
    Output: list dict lokasi (RAW, bisa ada yang belum punya koordinat).
    """
    if _pg_enabled() and pg_get_data_lokasi is not None:
        try:
            return pg_get_data_lokasi() or []
        except Exception as e:
            print(f"[PG] get_data_lokasi_any error: {e}")
    return []


def _is_active_row(row: dict) -> bool:
    """Normalisasi flag is_active (True jika kolom belum ada/None).

    - False / 'false' / '0' / 'no' dianggap nonaktif.
    """
    try:
        v = row.get('is_active')
    except Exception:
        return True

    if v is False:
        return False
    if isinstance(v, str) and v.strip().lower() in ('false', '0', 'no', 'n'):
        return False
    return True

def get_ref_jenis_lokasi_any() -> list:
    if _pg_enabled() and pg_get_ref_jenis_lokasi is not None:
        try:
            return pg_get_ref_jenis_lokasi() or []
        except Exception as e:
            print(f"[PG] get_ref_jenis_lokasi_any error: {e}")
    return []

try:
    _TZ_WIB = ZoneInfo("Asia/Jakarta")
except Exception:
    _TZ_WIB = datetime.timezone(datetime.timedelta(hours=7))


def _parse_waktu_form(v: str):
    """
    Input HTML datetime-local biasanya tanpa timezone (WIB).
    Kita ubah jadi UTC naive datetime supaya konsisten.
    """
    s = (v or "").strip()
    if not s:
        return None
    try:
        dt_local = datetime.datetime.fromisoformat(s)
    except Exception:
        return None
    if dt_local.tzinfo is None:
        dt_local = dt_local.replace(tzinfo=_TZ_WIB)
    return dt_local.astimezone(datetime.timezone.utc).replace(tzinfo=None)

def get_ref_kabkota_any() -> list:
    if _pg_enabled() and pg_get_ref_kabkota is not None:
        try:
            return pg_get_ref_kabkota() or []
        except Exception as e:
            print(f"[PG] get_ref_kabkota_any error: {e}")
    return []

def get_ref_status_lokasi_any() -> list:
    if _pg_enabled() and pg_get_ref_status_lokasi is not None:
        try:
            return pg_get_ref_status_lokasi() or []
        except Exception as e:
            print(f"[PG] get_ref_status_lokasi_any error: {e}")
    return []

def get_ref_tingkat_akses_any() -> list:
    if _pg_enabled() and pg_get_ref_tingkat_akses is not None:
        try:
            return pg_get_ref_tingkat_akses() or []
        except Exception as e:
            print(f"[PG] get_ref_tingkat_akses_any error: {e}")
    return []

def get_ref_kondisi_any() -> list:
    if _pg_enabled() and pg_get_ref_kondisi is not None:
        try:
            return pg_get_ref_kondisi() or []
        except Exception as e:
            print(f"[PG] get_ref_kondisi_any error: {e}")
    return []

def get_relawan_list_any() -> list:
    """Ambil daftar relawan dari Postgres untuk login."""
    if _pg_enabled() and pg_get_relawan_list is not None:
        try:
            lst = pg_get_relawan_list() or []
            # Urutkan A-Z untuk dropdown login
            try:
                lst.sort(key=lambda x: (x.get("nama_relawan") or "").strip().lower())
            except Exception:
                pass
            return lst
        except Exception as e:
            print(f"[PG] get_relawan_list_any error: {e}")
    return []


def write_lokasi_relawan_any(data: dict) -> bool:
    """Simpan absensi relawan (lokasi_relawan) ke Postgres."""
    if not _pg_enabled() or pg_insert_lokasi_relawan is None:
        return False
    try:
        return bool(
            pg_insert_lokasi_relawan(
                id_relawan=str(data.get("id_relawan") or "UNKNOWN"),
                latitude=data.get("latitude"),
                longitude=data.get("longitude"),
                catatan=data.get("catatan"),
                lokasi=data.get("lokasi"),
                lokasi_posko=data.get("lokasi_posko"),
                photo_link=data.get("photo_link"),
                waktu=data.get("waktu"),
            )
        )
    except Exception as e:
        print(f"[PG] write_lokasi_relawan_any error: {e}")
        return False


def get_next_id(table_env: str, default_table: str, id_col: str, prefix: str) -> str:
    """Wrapper generator ID biar tetap gaya lama (R0001, dst)."""
    if not _pg_enabled() or pg_next_id is None:
        return f"{prefix}{datetime.datetime.now().strftime('%m%d%H%M')}"
    try:
        return pg_next_id(table_env, default_table, id_col, prefix)
    except Exception:
        return f"{prefix}{datetime.datetime.now().strftime('%m%d%H%M')}"

def sanitize_for_log(text):
    """Sanitasi teks untuk mencegah injection dan karakter berbahaya di log."""
    if not text:
        return ""
    # Hapus karakter kontrol dan newline yang tidak aman
    text = str(text).replace("\n", " ").replace("\r", " ")
    # Hapus karakter kontrol lainnya
    text = "".join(char for char in text if ord(char) >= 32 or char in "\t")
    # Batasi panjang untuk mencegah log terlalu besar
    return text[:500]


def get_log_directory():
    """Mendapatkan direktori log yang aman (di luar web root)."""
    # Buat folder logs di root project (sibling dengan app.py)
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)

    # Buat file .htaccess untuk Apache (jika menggunakan Apache)
    htaccess_file = log_dir / ".htaccess"
    if not htaccess_file.exists():
        try:
            with open(htaccess_file, "w", encoding="utf-8") as f:
                f.write("# Deny access to log files\n")
                f.write("Order deny,allow\n")
                f.write("Deny from all\n")
        except Exception:
            pass  # Jika gagal (misalnya bukan Apache), abaikan

    # Buat file web.config untuk IIS (jika menggunakan IIS)
    webconfig_file = log_dir / "web.config"
    if not webconfig_file.exists():
        try:
            with open(webconfig_file, "w", encoding="utf-8") as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write("<configuration>\n")
                f.write("  <system.webServer>\n")
                f.write("    <authorization>\n")
                f.write('      <deny users="*" />\n')
                f.write("    </authorization>\n")
                f.write("  </system.webServer>\n")
                f.write("</configuration>\n")
        except Exception:
            pass  # Jika gagal, abaikan

    # Buat file README untuk dokumentasi
    readme_file = log_dir / "README.txt"
    if not readme_file.exists():
        try:
            with open(readme_file, "w", encoding="utf-8") as f:
                f.write("FOLDER LOGS - JANGAN HAPUS\n")
                f.write("=" * 50 + "\n\n")
                f.write("Folder ini berisi log aktivitas permintaan posko.\n")
                f.write("File log.txt mencatat semua permintaan logistik yang masuk.\n\n")
                f.write("KEAMANAN:\n")
                f.write("- Folder ini TIDAK bisa diakses melalui web browser\n")
                f.write("- Hanya bisa diakses melalui server/file system\n")
                f.write("- File log.txt berisi data sensitif, jangan share ke publik\n\n")
                f.write("PENTING: Jangan commit folder logs/ ke repository Git!\n")
        except Exception:
            pass

    return log_dir


def log_permintaan_posko(data_permintaan, nama_relawan, id_relawan, nama_posko=None):
    """Log permintaan posko ke file log.txt dengan format yang aman."""
    try:
        log_dir = get_log_directory()
        log_file = log_dir / "log.txt"

        # Sanitasi semua input
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nama_relawan_safe = sanitize_for_log(nama_relawan)
        id_relawan_safe = sanitize_for_log(id_relawan)
        id_permintaan = sanitize_for_log(data_permintaan.get("id_permintaan", "UNKNOWN"))
        kode_posko = sanitize_for_log(data_permintaan.get("kode_posko", "UNKNOWN"))
        nama_posko_safe = sanitize_for_log(nama_posko) if nama_posko else kode_posko
        keterangan = sanitize_for_log(data_permintaan.get("keterangan", ""))
        status = sanitize_for_log(data_permintaan.get("status", "Usulan"))
        tanggal = sanitize_for_log(data_permintaan.get("tanggal", ""))

        # Format log yang terstruktur dan mudah dibaca
        log_entry = (
            f"[{timestamp}] PERMINTAAN_POSKO | "
            f"ID: {id_permintaan} | "
            f"Peminta: {nama_relawan_safe} (ID: {id_relawan_safe}) | "
            f"Posko: {nama_posko_safe} ({kode_posko}) | "
            f"Status: {status} | "
            f"Tanggal: {tanggal} | "
            f"Keterangan: {keterangan if keterangan else '(tidak ada)'}"
        )

        # Tulis ke file dengan mode append
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")

    except Exception as e:
        # Jangan crash aplikasi jika logging gagal, cukup print error
        print(f"Error writing to log file: {e}")


# ==============================================================================
# KEAMANAN: Blokir akses ke folder logs melalui URL
# ==============================================================================
@app.route("/logs/<path:filename>")
def block_logs_access(filename):
    """Blokir akses ke file log melalui URL."""
    return "Access Denied", 403


# ==============================================================================
# API ENDPOINT: Refresh Data Map
# ==============================================================================
@app.route("/api/refresh_map", methods=["GET"])
def api_refresh_map():
    """API endpoint untuk mendapatkan data map terbaru."""
    try:
        data_lokasi_raw = get_data_lokasi_any()
        data_lokasi = [d for d in data_lokasi_raw if d.get("latitude") and d.get("longitude") and _is_active_row(d)]

        status_map = get_status_map_any()

        # Lokasi relawan (Postgres) - marker di map
        relawan_lokasi = []
        if pg_get_relawan_locations_last24h:
            try:
                relawan_lokasi = pg_get_relawan_locations_last24h(720)
            except Exception as e:
                print(f"Warning: gagal ambil lokasi_relawan dari Postgres: {e}")

        
        # Permintaan logistik (Postgres) - marker di map (ambil last 24 jam)
        permintaan_logistik = []
        if pg_get_logistik_permintaan_last24h:
            try:
                permintaan_logistik = pg_get_logistik_permintaan_last24h(720) or []
            except Exception as e:
                print(f"Warning: gagal ambil logistik_permintaan dari Postgres: {e}")

        return jsonify(
            {
                "success": True,
                "data_lokasi": data_lokasi,
                "status_map": status_map,
                "relawan_lokasi": relawan_lokasi,
                "asesmen_kesehatan": pg_get_asesmen_kesehatan_last24h(720) if pg_get_asesmen_kesehatan_last24h else [],
                "asesmen_pendidikan": pg_get_asesmen_pendidikan_last24h(720) if pg_get_asesmen_pendidikan_last24h else [],
                "asesmen_psikososial": pg_get_asesmen_psikososial_last24h(720) if pg_get_asesmen_psikososial_last24h else [],
                "asesmen_infrastruktur": pg_get_asesmen_infrastruktur_last24h(720) if pg_get_asesmen_infrastruktur_last24h else [],
                "asesmen_wash": pg_get_asesmen_wash_last24h(720) if pg_get_asesmen_wash_last24h else [],
                "asesmen_kondisi": pg_get_asesmen_kondisi_last24h(720) if pg_get_asesmen_kondisi_last24h else [],

                "permintaan_logistik": permintaan_logistik
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==============================================================================
# ROUTE UTAMA
# ==============================================================================
@app.route("/")
def map_view():
    # Pastikan GeoJSON kab/kota siap (dipakai cek wilayah absensi)
    ensure_kabkota_geojson_ready()

    data_lokasi_raw = get_data_lokasi_any()
    data_lokasi = [d for d in data_lokasi_raw if d.get("latitude") and d.get("longitude") and _is_active_row(d)]

    # Opsional: stok gudang / master logistik / rekap (kalau ada tabelnya)
    stok_gudang = []
    try:
        stok_gudang = get_logistik_keluar_grouped()
    except Exception as e:
        print(f"[SHEET] get_stok_gudang error: {e}")

    # dd(stok_gudang)

    status_map = get_status_map_any()

    rekap_kabkota = []
    try:
        # Panggil fungsi baru tadi
        rekap_kabkota = get_rekap_from_spreadsheet()
    except Exception as e:
        print(f"[SHEET] get_rekap_kabkota error: {e}")

    latest_rekap = {}
    for row in rekap_kabkota:
        kabkota = row.get("kabkota") or row.get("kabupaten_kota") or row.get("nama_kabkota")
        if kabkota and kabkota not in latest_rekap:
            latest_rekap[kabkota] = row

    data_relawan = get_relawan_list_any()

    # Ambil daftar posko untuk form permintaan dengan nama dan kode
    data_posko_list = []
    for d in data_lokasi:
        if d.get("jenis_lokasi") == "Posko Pengungsian":
            kode = d.get("kode_lokasi") or d.get("id_lokasi") or ""
            kab = d.get("nama_kabkota") or d.get("kabupaten_kota") or ""
            nm = d.get("nama_lokasi") or ""
            data_posko_list.append({"kode": kode, "nama": nm, "kabkota":kab})

    # urutkan: Kab/Kota A-Z, lalu kode A-Z
    data_posko_list.sort(
        key=lambda x: (
            (x.get("kabkota") or "").strip().lower(),
            (x.get("kode") or "").strip().lower(),
            (x.get("nama") or "").strip().lower(),
        )
    )

    # Lokasi relawan (Postgres) - marker di map (ambil last 24 jam)
    relawan_lokasi = []
    if pg_get_relawan_locations_last24h:
        try:
            relawan_lokasi = pg_get_relawan_locations_last24h(720)
        except Exception as e:
            print(f"Warning: gagal ambil lokasi_relawan dari Postgres: {e}")

    # Permintaan logistik (Postgres) - marker di map (ambil last 24 jam)
    permintaan_logistik = []
    if pg_get_logistik_permintaan_last24h:
        try:
            permintaan_logistik = pg_get_logistik_permintaan_last24h(720)
        except Exception as e:
            print(f"Warning: gagal ambil logistik_permintaan dari Postgres: {e}")

    data_barang = []
    if _pg_enabled() and pg_get_master_logistik_codes is not None:
        try:
            data_barang = pg_get_master_logistik_codes() or []
        except Exception as e:
            print(f"[PG] get_master_logistik_codes error: {e}")
    # --- dropdown refs untuk INPUT LOKASI (data_lokasi) ---
    ref_jenis_lokasi = []
    ref_kabkota = []
    ref_status_lokasi = []
    ref_tingkat_akses = []
    ref_kondisi = []
    if _pg_enabled():
        try:
            ref_jenis_lokasi = (pg_get_ref_jenis_lokasi() or [])
        except Exception:
            ref_jenis_lokasi = []
        try:
            ref_kabkota = (pg_get_ref_kabkota() or [])
        except Exception:
            ref_kabkota = []
        try:
            ref_status_lokasi = (pg_get_ref_status_lokasi() or [])
        except Exception:
            ref_status_lokasi = []
        try:
            ref_tingkat_akses = (pg_get_ref_tingkat_akses() or [])
        except Exception:
            ref_tingkat_akses = []
        try:
            ref_kondisi = (pg_get_ref_kondisi() or [])
        except Exception:
            ref_kondisi = []

    return render_template(
        "map.html",
        data_lokasi=json.dumps(data_lokasi),
        stok_gudang=stok_gudang,
        rekap_kabkota=rekap_kabkota,
        relawan_lokasi=json.dumps(relawan_lokasi),
        relawan_list=data_relawan,
        data_posko=data_posko_list,
        data_barang=data_barang,
        logged_in=session.get("logged_in", False),
        nama_relawan=session.get("nama_relawan", ""),
        is_admin=session.get("is_admin", False),
        status_map=json.dumps(status_map),
        asesmen_kesehatan=json.dumps(pg_get_asesmen_kesehatan_last24h(720) if pg_get_asesmen_kesehatan_last24h else []),
        asesmen_pendidikan=json.dumps(pg_get_asesmen_pendidikan_last24h(720) if pg_get_asesmen_pendidikan_last24h else []),
        asesmen_psikososial=json.dumps(pg_get_asesmen_psikososial_last24h(720) if pg_get_asesmen_psikososial_last24h else []),
        asesmen_infrastruktur=json.dumps(pg_get_asesmen_infrastruktur_last24h(720) if pg_get_asesmen_infrastruktur_last24h else []),
        asesmen_wash=json.dumps(pg_get_asesmen_wash_last24h(720) if pg_get_asesmen_wash_last24h else []),
        ref_jenis_lokasi=ref_jenis_lokasi,
        ref_kabkota=ref_kabkota,
        ref_status_lokasi=ref_status_lokasi,
        ref_tingkat_akses=ref_tingkat_akses,
        ref_kondisi=ref_kondisi,
        asesmen_kondisi=json.dumps(pg_get_asesmen_kondisi_last24h(720) if pg_get_asesmen_kondisi_last24h else []),
        permintaan_logistik=json.dumps(permintaan_logistik)
    )


@app.route("/rekap_asesmen")
def rekap_asesmen():
    """Halaman rekap data asesmen per kabupaten/kota."""
    # Pastikan GeoJSON kab/kota siap
    ensure_kabkota_geojson_ready()
    return render_template("rekap_asesmen.html")


@app.route("/api/rekap_asesmen", methods=["POST"])
def api_rekap_asesmen():
    """API endpoint untuk mendapatkan data rekap asesmen berdasarkan filter."""
    # Pastikan GeoJSON kab/kota siap
    ensure_kabkota_geojson_ready()
    try:
        data = request.get_json() or {}
        
        jenis_asesmen = data.get("jenis_asesmen") or None
        # tanggal_dari_str = data.get("tanggal_dari")
        # tanggal_sampai_str = data.get("tanggal_sampai")
        tanggal_dari = data.get("tanggal_dari")
        tanggal_sampai = data.get("tanggal_sampai")
        status_filter = data.get("status") or None
        
        # Parse tanggal
        # tanggal_dari = None
        # tanggal_sampai = None
        
        # if tanggal_dari_str:
        #     try:
        #         # Format: dd/mm/yyyy
        #         parts = tanggal_dari_str.split("/")
        #         if len(parts) == 3:
        #             tanggal_dari = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
        #     except Exception:
        #         pass
        
        # if tanggal_sampai_str:
        #     try:
        #         # Format: dd/mm/yyyy
        #         parts = tanggal_sampai_str.split("/")
        #         if len(parts) == 3:
        #             tanggal_sampai = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
        #     except Exception:
        #         pass
        
        # Normalisasi status filter
        if status_filter and status_filter.lower() == "semua":
            status_filter = None
        
        if not pg_get_asesmen_rekap_by_kabkota:
            return jsonify({
                "success": False,
                "error": "Fungsi rekap belum tersedia"
            }), 500
        
        rekap_data = pg_get_asesmen_rekap_by_kabkota(
            jenis_asesmen=jenis_asesmen,
            tanggal_dari=tanggal_dari,
            tanggal_sampai=tanggal_sampai,
            status_filter=status_filter,
            app_root_path=app.root_path,
        )
        
        # Convert ke format list untuk frontend
        result = []
        for kabkota, stats in rekap_data.items():
            total = stats.get("total", 0)
            valid = stats.get("valid", 0)
            pending = stats.get("pending", 0)
            ditolak_error = stats.get("ditolak_error", 0)
            
            # Hitung persentase valid
            persentase_valid = (valid / total * 100) if total > 0 else 0
            
            result.append({
                "kabkota": kabkota,
                "total": total,
                "valid": valid,
                "pending": pending,
                "ditolak_error": ditolak_error,
                "persentase_valid": round(persentase_valid, 1),
            })
        
        # Sort by kabkota name
        result.sort(key=lambda x: x.get("kabkota", ""))
        
        return jsonify({
            "success": True,
            "data": result
        })
    
    except Exception as e:
        print(f"[API] Error rekap asesmen: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/api/rekap_asesmen_detail", methods=["POST"])
def api_rekap_asesmen_detail():
    """API endpoint untuk mendapatkan detail statistik per kabupaten/kota."""
    # Pastikan GeoJSON kab/kota siap
    ensure_kabkota_geojson_ready()
    try:
        data = request.get_json() or {}
        
        kabkota = data.get("kabkota")
        jenis_asesmen = data.get("jenis_asesmen") or None
        tanggal_dari_str = data.get("tanggal_dari")
        tanggal_sampai_str = data.get("tanggal_sampai")
        status_filter = data.get("status") or None
        
        if not kabkota:
            return jsonify({
                "success": False,
                "error": "Kabupaten/kota harus diisi"
            }), 400
        
        if status_filter and status_filter.lower() == "semua":
            status_filter = None
        
        # Biarkan pg_data menangani parsing & normalisasi tanggal
        tanggal_dari = tanggal_dari_str
        tanggal_sampai = tanggal_sampai_str
        
        if not pg_get_asesmen_rekap_by_kabkota:
            return jsonify({
                "success": False,
                "error": "Fungsi rekap belum tersedia"
            }), 500
        
        rekap_data = pg_get_asesmen_rekap_by_kabkota(
            jenis_asesmen=jenis_asesmen,
            tanggal_dari=tanggal_dari,
            tanggal_sampai=tanggal_sampai,
            status_filter=status_filter,
            app_root_path=app.root_path,
        )
        
        stats = rekap_data.get(kabkota, {})
        detail_asesmen = stats.get("detail", [])
        
        # Mapping jenis asesmen ke label
        jenis_label_map = {
            "kesehatan": "Asesmen Kesehatan",
            "pendidikan": "Asesmen Pendidikan",
            "psikososial": "Asesmen Psikososial",
            "infrastruktur": "Asesmen Infrastruktur",
            "wash": "Asesmen WASH",
            "kondisi": "Asesmen Kondisi",
        }
        
        # Group by jenis asesmen untuk menghitung nomor urut
        jenis_count = {}
        asesmen_list = []
        
        for asesmen in detail_asesmen:
            jenis = asesmen.get("jenis_asesmen", "")
            if jenis not in jenis_count:
                jenis_count[jenis] = 0
            jenis_count[jenis] += 1
            
            asesmen_list.append({
                "id": asesmen.get("id"),
                "jenis_asesmen": jenis,
                "jenis_label": jenis_label_map.get(jenis, jenis.capitalize()),
                "nomor_urut": jenis_count[jenis],
                "status": asesmen.get("status", "-"),
                "skor": asesmen.get("skor"),
                "waktu": asesmen.get("waktu"),
                "id_relawan": asesmen.get("id_relawan"),
                "nama_relawan": asesmen.get("nama_relawan"),
                "latitude": asesmen.get("latitude"),
                "longitude": asesmen.get("longitude"),
            })
        
        # Sort by waktu (terbaru dulu)
        asesmen_list.sort(key=lambda x: str(x.get("waktu") or ""), reverse=True)
        
        return jsonify({
            "success": True,
            "data": {
                "kabkota": kabkota,
                "asesmen_list": asesmen_list,
            }
        })
    
    except Exception as e:
        print(f"[API] Error rekap detail: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# ==============================================================================
# 1. LOGIKA LOGIN
# ==============================================================================
@app.route("/login", methods=["POST"])
def login():
    nama = request.form.get("nama")
    kode_akses = request.form.get("kode_akses")

    # Basic validation
    if not nama or not kode_akses:
        flash("Login gagal: Nama dan Kode Akses harus diisi.", "danger")
        return redirect(url_for("map_view"))

    relawan_list = get_relawan_list_any()

    # Cari relawan sesuai nama (case-insensitive)
    matched = None
    for r in relawan_list:
        if r.get("nama_relawan", "").strip().lower() == (nama or "").strip().lower():
            matched = r
            break

    if not matched:
        flash(f"Login gagal: Nama relawan '{nama}' tidak ditemukan di database.", "danger")
        return redirect(url_for("map_view"))

    expected_code = (matched.get("kode_akses") or "").strip()
    if not expected_code:
        flash("Login gagal: Relawan belum memiliki kode akses terdaftar. Hubungi admin.", "danger")
        return redirect(url_for("map_view"))

    # Cocokkan kode akses sesuai relawan (case-insensitive)
    if kode_akses.strip().lower() != expected_code.lower():
        flash("Login gagal: Kode akses salah.", "danger")
        return redirect(url_for("map_view"))

    # Login berhasil
    session["logged_in"] = True
    session["nama_relawan"] = matched["nama_relawan"]
    session["id_relawan"] = matched.get("id_relawan", "UNKNOWN")
    session["is_admin"] = bool(matched.get("is_admin") or False)
    flash(f"Login berhasil! Selamat bertugas, {matched['nama_relawan']}.", "success")
    return redirect(url_for("map_view"))


@app.route("/logout", methods=["POST"])
def logout():
    session.pop("logged_in", None)
    session.pop("nama_relawan", None)
    session.pop("id_relawan", None)
    session.pop("is_admin", None)
    flash("Logout Berhasil.", "success")
    return redirect(url_for("map_view"))


# ==============================================================================
# 2. LOGIKA PERMINTAAN LOGISTIK (logistik_permintaan)
# ==============================================================================
@app.route("/submit_permintaan", methods=["POST"])
def submit_permintaan():
    if not session.get("logged_in"):
        return redirect(url_for("map_view"))

    if not _pg_enabled() or pg_insert_logistik_permintaan is None:
        flash("Fitur permintaan belum aktif: tabel/pg_data belum siap.", "danger")
        return redirect(url_for("map_view"))

    kode_posko = request.form.get("kode_posko")
    keterangan = request.form.get("keterangan", "")
    latitude = request.form.get("latitude")
    longitude = request.form.get("longitude")
    waktu_utc = _parse_waktu_form(request.form.get("waktu"))

    # Ambil nama posko untuk log yang lebih informatif
    nama_posko = None
    data_lokasi_raw = get_data_lokasi_any()
    for lokasi in data_lokasi_raw:
        if lokasi.get("kode_lokasi") == kode_posko:
            nama_posko = lokasi.get("nama_lokasi") or lokasi.get("kabupaten_kota") or kode_posko
            break

    data = {
        # id untuk log saja (di DB pakai bigserial)
        "id_permintaan": f"LP-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}",
        "tanggal": datetime.datetime.now().strftime("%d-%B-%y"),
        "kode_posko": kode_posko,
        "keterangan": keterangan,
        "status_permintaan": "Usulan",
        # kompatibel dengan logger lama
        "status": "Usulan",
        "kode_barang": "-",
        "jumlah_diminta": "-",
        "id_relawan": session.get("id_relawan", session.get("nama_relawan")),
        "relawan": session.get("id_relawan", session.get("nama_relawan")),
        "photo_link": "",
        "latitude": latitude,
        "longitude": longitude,
        "waktu": waktu_utc,
    }

    try:
        pg_insert_logistik_permintaan(data)
    except Exception as e:
        flash(f"Gagal simpan permintaan: {e}", "danger")
        return redirect(url_for("map_view"))

    # Log permintaan ke file log.txt
    nama_relawan = session.get("nama_relawan", "UNKNOWN")
    id_relawan = session.get("id_relawan", "UNKNOWN")
    log_permintaan_posko(data, nama_relawan, id_relawan, nama_posko)

    flash(f"Permintaan {nama_posko or kode_posko} berhasil dikirim!", "success")
    return redirect(url_for("map_view"))



# ==============================================================================
# 2b. ADMIN: UPDATE STATUS PERMINTAAN LOGISTIK (logistik_permintaan)
# ==============================================================================
@app.route("/api/update_permintaan_status", methods=["POST"])
def api_update_permintaan_status():
    """Update status_permintaan pada logistik_permintaan.
    Hanya untuk relawan yang login dan is_admin=True.
    Payload: {id: <int>, status: <str>, note?: <str>}
    """
    if not session.get("logged_in"):
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "Forbidden"}), 403

    if not _pg_enabled() or pg_update_logistik_permintaan_status is None:
        return jsonify({"success": False, "error": "Fitur belum aktif (pg_data belum siap)."}), 500

    payload = request.get_json(silent=True) or {}
    if not payload:
        payload = request.form.to_dict() if request.form else {}

    # fallback: kalau front-end ngirim id_permintaan / permintaan_id
    id_raw = payload.get("id")
    if id_raw is None:
        id_raw = payload.get("id_permintaan")
    if id_raw is None:
        id_raw = payload.get("permintaan_id")

    status_new = (payload.get("status") or payload.get("status_permintaan") or "").strip()
    note = (payload.get("note") or "").strip() or None

    allowed = {"Usulan","Draft", "Diproses", "Dikirim", "Diterima", "Ditolak"}
    if status_new not in allowed:
        return jsonify({"success": False, "error": "Status tidak valid."}), 400

    try:
        id_int = int(str(id_raw).strip())
    except Exception:
        return jsonify({"success": False, "error": "ID tidak valid."}), 400

    try:
        ok = pg_update_logistik_permintaan_status(
            id_int,
            status_new,
            actor_id_relawan=session.get("id_relawan"),
            actor_nama_relawan=session.get("nama_relawan"),
            note=note,
        )

        if not ok:
            return jsonify({"success": False, "error": "Data permintaan tidak ditemukan."}), 404

        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ==============================================================================


# ==============================================================================
# ==============================================================================
# 2c. ADMIN: SET ASESMEN is_active (toggle True/False)
# ==============================================================================
@app.route("/api/set_asesmen_active", methods=["POST"])
def api_set_asesmen_active():
    """Set is_active True/False pada asesmen tertentu.

    Hanya untuk relawan yang login dan is_admin=True.
    Payload: {kind: 'kesehatan|pendidikan|psikososial|infrastruktur|wash|kondisi', id: <int>, is_active: true/false, note?: <str>}
    """
    if not session.get("logged_in"):
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "Forbidden"}), 403

    if not _pg_enabled() or pg_set_asesmen_active is None:
        return jsonify({"success": False, "error": "Fitur belum aktif (pg_data belum siap)."}), 500

    payload = request.get_json(silent=True) or {}
    if not payload:
        payload = request.form.to_dict() if request.form else {}

    kind = (payload.get("kind") or "").strip()
    asesmen_id = payload.get("id")
    is_active_raw = payload.get("is_active")
    note = (payload.get("note") or "").strip() or None

    if not kind or asesmen_id is None or is_active_raw is None:
        return jsonify({"success": False, "error": "Payload tidak lengkap."}), 400

    # Normalisasi bool
    if isinstance(is_active_raw, str):
        is_active_val = is_active_raw.strip().lower() in ("1", "true", "yes", "y", "on")
    else:
        is_active_val = bool(is_active_raw)

    try:
        ok = pg_set_asesmen_active(
            kind=kind,
            asesmen_id=asesmen_id,
            is_active=is_active_val,
            actor_id_relawan=session.get("id_relawan"),
            actor_nama_relawan=session.get("nama_relawan"),
            note=note,
        )

        if ok:
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "Data tidak ditemukan."}), 404

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


# Backward compatible (dipakai patch sebelumnya)
@app.route("/api/deactivate_asesmen", methods=["POST"])
def api_deactivate_asesmen():
    """Alias untuk set is_active=false."""
    if not session.get("logged_in"):
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "Forbidden"}), 403

    if not _pg_enabled() or pg_set_asesmen_active is None:
        return jsonify({"success": False, "error": "Fitur belum aktif (pg_data belum siap)."}), 500

    payload = request.get_json(silent=True) or {}
    if not payload:
        payload = request.form.to_dict() if request.form else {}

    kind = (payload.get("kind") or "").strip()
    asesmen_id = payload.get("id")
    note = (payload.get("note") or "").strip() or None

    if not kind or asesmen_id is None:
        return jsonify({"success": False, "error": "Payload tidak lengkap."}), 400

    try:
        ok = pg_set_asesmen_active(
            kind=kind,
            asesmen_id=asesmen_id,
            is_active=False,
            actor_id_relawan=session.get("id_relawan"),
            actor_nama_relawan=session.get("nama_relawan"),
            note=note,
        )
        if ok:
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "Data tidak ditemukan."}), 404
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


# ==============================================================================
# 2d. ADMIN: LIST ASESMEN (AKTIF + NONAKTIF) UNTUK PANEL ADMIN
# ==============================================================================
@app.route("/api/admin_asesmen_list", methods=["GET"])
def api_admin_asesmen_list():
    """Ambil daftar asesmen (aktif + nonaktif) untuk panel admin."""
    if not session.get("logged_in"):
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "Forbidden"}), 403

    if not _pg_enabled() or pg_get_admin_asesmen_list is None:
        return jsonify({"success": False, "error": "Fitur belum aktif (pg_data belum siap)."}), 500

    hours_raw = request.args.get("hours", "24")
    try:
        hours = int(str(hours_raw).strip())
    except Exception:
        hours = 24

    start = request.args.get("start", "")
    end   = request.args.get("end", "")
    kind  = request.args.get("kind", "")

    limit_raw = request.args.get("limit", "10")
    try:
        limit = int(str(limit_raw).strip())
    except Exception:
        limit = 10

    offset_raw = request.args.get("offset", "0")
    try:
        offset = int(str(offset_raw).strip())
    except Exception:
        offset = 0

    try:
        result = pg_get_admin_asesmen_list(
            hours=hours, 
            limit_per_kind=500, 
            start=start, 
            end=end, 
            kind_filter=kind,
            offset=offset,
            limit=limit
        )
        # result is already {"success": True, "rows": [...], "has_more": ...}
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==============================================================================
# 2e. ADMIN: BACA LOG AKSI ADMIN
# ==============================================================================
@app.route("/api/admin_action_logs", methods=["GET"])
def api_admin_action_logs():
    """Ambil log aksi admin (untuk ditampilkan di modal admin)."""
    if not session.get("logged_in"):
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "Forbidden"}), 403

    if not _pg_enabled() or pg_get_admin_action_logs is None:
        return jsonify({"success": False, "error": "Fitur belum aktif (pg_data belum siap)."}), 500

    limit_raw = request.args.get("limit", "200")
    try:
        limit = int(str(limit_raw).strip())
    except Exception:
        limit = 200

    try:
        logs = pg_get_admin_action_logs(limit)
        return jsonify({"success": True, "logs": logs})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==============================================================================
# 2e. ADMIN: DATA LOKASI (is_active + update jenis_lokasi)
# ==============================================================================
@app.route("/api/admin_lokasi_list", methods=["GET"])
def api_admin_lokasi_list():
    """Ambil daftar data_lokasi (aktif + nonaktif) untuk panel admin.

    Query: limit (default 500)
    """
    if not session.get("logged_in"):
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "Forbidden"}), 403

    if not _pg_enabled() or pg_get_admin_lokasi_list is None:
        return jsonify({"success": False, "error": "Fitur belum aktif (pg_data belum siap)."}), 500

    limit_raw = request.args.get("limit", "10")
    try:
        limit = int(str(limit_raw).strip())
    except Exception:
        limit = 10

    offset_raw = request.args.get("offset", "0")
    try:
        offset = int(str(offset_raw).strip())
    except Exception:
        offset = 0

    search = request.args.get("search", "").strip()
    kind = request.args.get("kind", "").strip()
    start = request.args.get("start", "").strip() or None
    end = request.args.get("end", "").strip() or None

    try:
        result = pg_get_admin_lokasi_list(
            limit=limit, 
            offset=offset, 
            search=search, 
            kind=kind, 
            start=start, 
            end=end
        )
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/set_lokasi_active", methods=["POST"])
def api_set_lokasi_active():
    """Set is_active True/False pada data_lokasi tertentu.

    Payload: {id_lokasi: <str>, is_active: true/false, note?: <str>}
    """
    if not session.get("logged_in"):
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "Forbidden"}), 403

    if not _pg_enabled() or pg_set_data_lokasi_active is None:
        return jsonify({"success": False, "error": "Fitur belum aktif (pg_data belum siap)."}), 500

    payload = request.get_json(silent=True) or {}
    if not payload:
        payload = request.form.to_dict() if request.form else {}

    id_lokasi = (payload.get("id_lokasi") or payload.get("id") or "").strip()
    is_active_raw = payload.get("is_active")
    note = (payload.get("note") or "").strip() or None

    if not id_lokasi or is_active_raw is None:
        return jsonify({"success": False, "error": "Payload tidak lengkap."}), 400

    if isinstance(is_active_raw, str):
        is_active_val = is_active_raw.strip().lower() in ("1", "true", "yes", "y", "on")
    else:
        is_active_val = bool(is_active_raw)

    try:
        ok = pg_set_data_lokasi_active(
            id_lokasi=id_lokasi,
            is_active=is_active_val,
            actor_id_relawan=session.get("id_relawan"),
            actor_nama_relawan=session.get("nama_relawan"),
            note=note,
        )
        if ok:
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "Data tidak ditemukan."}), 404
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


@app.route("/api/update_lokasi_jenis", methods=["POST"])
def api_update_lokasi_jenis():
    """Update jenis_lokasi pada data_lokasi.

    Payload: {id_lokasi: <str>, jenis_lokasi: <str>, note?: <str>}
    """
    if not session.get("logged_in"):
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    if not session.get("is_admin"):
        return jsonify({"success": False, "error": "Forbidden"}), 403

    if not _pg_enabled() or pg_update_data_lokasi_jenis is None:
        return jsonify({"success": False, "error": "Fitur belum aktif (pg_data belum siap)."}), 500

    payload = request.get_json(silent=True) or {}
    if not payload:
        payload = request.form.to_dict() if request.form else {}

    id_lokasi = (payload.get("id_lokasi") or payload.get("id") or "").strip()
    jenis_lokasi = (payload.get("jenis_lokasi") or "").strip()
    note = (payload.get("note") or "").strip() or None

    if not id_lokasi or not jenis_lokasi:
        return jsonify({"success": False, "error": "Payload tidak lengkap."}), 400

    try:
        ok = pg_update_data_lokasi_jenis(
            id_lokasi=id_lokasi,
            jenis_lokasi=jenis_lokasi,
            actor_id_relawan=session.get("id_relawan"),
            actor_nama_relawan=session.get("nama_relawan"),
            note=note,
        )
        if ok:
            return jsonify({"success": True})
        return jsonify({"success": False, "error": "Data tidak ditemukan."}), 404
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


# ==============================================================================
# 3. LOGIKA ABSENSI RELAWAN
# ==============================================================================
def point_in_polygon(point, polygon):
    """Mengecek apakah titik (lon, lat) ada di dalam list koordinat polygon.
    Algoritma: Ray Casting.
    """
    x, y = point  # x = longitude, y = latitude
    inside = False
    n = len(polygon)

    p1x, p1y = polygon[0]
    for i in range(n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y

    return inside


def cek_wilayah_geojson(user_lat, user_lon):
    """Looping semua wilayah di GeoJSON Sumut untuk mencari user ada di mana."""
    try:
        # Pastikan file GeoJSON tersedia (kalau hilang/expired, generate lagi dari Postgres)
        ensure_kabkota_geojson_ready()

        json_path = os.path.join(app.root_path, "static", "data", "kabkota_sumut.json")

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        point = (float(user_lon), float(user_lat))

        for feature in data.get("features", []):
            geometry = feature.get("geometry", {})
            props = feature.get("properties", {})

            # Ambil nama kota (sesuaikan dengan nama kolom di GeoJSON)
            nama_wilayah = (
                props.get("kabkota")
                or props.get("KABKOTA")
                or props.get("NAMOBJ")
                or "Wilayah Tak Bernama"
            )

            found = False

            # GeoJSON bisa berupa 'Polygon' (1 pulau) atau 'MultiPolygon' (banyak pulau)
            if geometry.get("type") == "Polygon":
                poly_coords = geometry.get("coordinates", [[]])[0]
                if poly_coords and point_in_polygon(point, poly_coords):
                    found = True

            elif geometry.get("type") == "MultiPolygon":
                for poly in geometry.get("coordinates", []):
                    poly_coords = poly[0] if poly else []
                    if poly_coords and point_in_polygon(point, poly_coords):
                        found = True
                        break

            if found:
                return nama_wilayah

        return "Luar Wilayah Sumut"

    except Exception as e:
        print(f"Error Cek GeoJSON: {e}")
        return "Gagal Deteksi Wilayah"


def haversine_distance(lat1, lon1, lat2, lon2):
    """Return distance in kilometers between two lat/lon points."""
    try:
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(math.radians, (float(lat1), float(lon1), float(lat2), float(lon2)))
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return 6371.0 * c
    except Exception:
        return float("inf")


@app.route("/submit_absensi", methods=["POST"])
def submit_absensi():
    if not session.get("logged_in"):
        flash("Silahkan login terlebih dahulu!", "danger")
        return redirect(url_for("map_view"))

    latitude = request.form.get("latitude")
    longitude = request.form.get("longitude")
    catatan = request.form.get("catatan")
    waktu_utc = _parse_waktu_form(request.form.get("waktu"))

    lokasi_terdeteksi = "Mencari..."

    if latitude and longitude:
        lokasi_terdeteksi = cek_wilayah_geojson(latitude, longitude)

    # Cari posko terdekat berdasarkan koordinat absensi
    lokasi_posko_code = ""
    try:
        data_lokasi_raw = get_data_lokasi_any()

        # Prioritaskan Posko Pengungsian; jika tidak ada, gunakan semua lokasi dengan koordinat
        candidates = [
            l
            for l in data_lokasi_raw
            if l.get("jenis_lokasi") == "Posko Pengungsian" and l.get("latitude") and l.get("longitude")
        ]
        if not candidates:
            candidates = [l for l in data_lokasi_raw if l.get("latitude") and l.get("longitude")]

        min_dist = None
        nearest = None
        for p in candidates:
            try:
                plat = float(p.get("latitude"))
                plong = float(p.get("longitude"))
                dist = haversine_distance(latitude, longitude, plat, plong)
                if min_dist is None or dist < min_dist:
                    min_dist = dist
                    nearest = p
            except Exception:
                continue

        if nearest:
            lokasi_posko_code = nearest.get("kode_lokasi") or nearest.get("kode") or ""

    except Exception as e:
        print(f"Error mencari posko terdekat: {e}")

    data = {
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "id_relawan": session.get("id_relawan", "UNKNOWN"),
        "latitude": latitude,
        "longitude": longitude,
        "lokasi": lokasi_terdeteksi,
        "lokasi_posko": lokasi_posko_code,
        "catatan": catatan,
        "photo_link": "",
        "waktu": waktu_utc,
    }

    write_lokasi_relawan_any(data)
    msg_type = "success" if lokasi_terdeteksi != "Luar Wilayah Sumut" else "warning"
    flash(f"Absensi berhasil! Posisi Anda terdeteksi di: {lokasi_terdeteksi}", msg_type)
    return redirect(url_for("map_view"))


# ==============================================================================
# 4. ASESMEN (KESEHATAN & PENDIDIKAN) -> POSTGRES
# ==============================================================================
def _ask_int_1_5(val, default: int = 1) -> int:
    try:
        v = int(val)
        if v < 1:
            v = 1
        if v > 5:
            v = 5
        return v
    except Exception:
        return default



def _get_asesmen_photo_path(asesmen_name: str):
    """Simpan max 2 foto asesmen ke media/asesmen/ dan return nilai untuk kolom photo_path (JSON string array)."""
    files = request.files.getlist("photos")
    paths = save_asesmen_photos(
        files,
        asesmen_name=asesmen_name,
        id_relawan=session.get("id_relawan", "UNKNOWN"),
        media_root=MEDIA_DIR,
    )
    return photos_to_photo_path_value(paths)

def _require_login():
    if not session.get("logged_in"):
        flash("Silahkan login terlebih dahulu!", "danger")
        return False
    return True


@app.route("/submit_asesmen_kesehatan", methods=["POST"])
def submit_asesmen_kesehatan():
    if not _require_login():
        return redirect(url_for("map_view"))

    if not _pg_enabled() or pg_insert_asesmen_kesehatan is None:
        flash("Fitur asesmen belum aktif: DATABASE_URL/pg_data belum siap.", "danger")
        return redirect(url_for("map_view"))

    # Upload foto (opsional, maksimal 2)
    try:
        photo_path = _get_asesmen_photo_path('kesehatan')
    except Exception as e:
        flash(f"Gagal upload foto asesmen kesehatan: {e}", "danger")
        return redirect(url_for("map_view"))


    # Form data
    kode_posko = request.form.get("kode_posko") or None
    lat = request.form.get("latitude")
    lon = request.form.get("longitude")
    catatan = request.form.get("catatan") or None
    radius_in = request.form.get("radius")
    waktu_utc = _parse_waktu_form(request.form.get("waktu"))
    try:
        radius = float(radius_in) if radius_in not in (None, "") else 2.0
    except Exception:
        radius = 2.0
    if radius <= 0:
        radius = 2.0
    if radius > 50:
        radius = 50.0

    p1 = _ask_int_1_5(request.form.get("p1"))
    p2 = _ask_int_1_5(request.form.get("p2"))
    p3 = _ask_int_1_5(request.form.get("p3"))
    p4 = _ask_int_1_5(request.form.get("p4"))
    p5 = _ask_int_1_5(request.form.get("p5"))

    weights = {"p1": 1.0, "p2": 1.0, "p3": 1.0, "p4": 1.5, "p5": 1.5}
    answers = {"p1": p1, "p2": p2, "p3": p3, "p4": p4, "p5": p5}

    skor = (
        answers["p1"] * weights["p1"]
        + answers["p2"] * weights["p2"]
        + answers["p3"] * weights["p3"]
        + answers["p4"] * weights["p4"]
        + answers["p5"] * weights["p5"]
    )

    max_skor = (
        5 * weights["p1"]
        + 5 * weights["p2"]
        + 5 * weights["p3"]
        + 5 * weights["p4"]
        + 5 * weights["p5"]
    )
    skor_100 = (skor / max_skor) * 100.0

    if skor_100 >= 80:
        status = "Kritis"
    elif skor_100 >= 60:
        status = "Waspada"
    else:
        status = "Aman"

    try:
        pg_insert_asesmen_kesehatan(
            id_relawan=session.get("id_relawan", "UNKNOWN"),
            kode_posko=kode_posko,
            jawaban=answers,
            skor=float(skor_100),
            status=status,
            latitude=lat,
            longitude=lon,
            catatan=catatan,
            photo_path=photo_path,
            radius=radius,
            waktu=waktu_utc
        )
        flash(f"Asesmen Kesehatan tersimpan (Status: {status}, Skor: {skor_100:.1f}).", "success")
    except Exception as e:
        flash(f"Gagal simpan asesmen kesehatan: {e}", "danger")

    return redirect(url_for("map_view"))


@app.route("/submit_asesmen_pendidikan", methods=["POST"])
def submit_asesmen_pendidikan():
    if not _require_login():
        return redirect(url_for("map_view"))

    if not _pg_enabled() or pg_insert_asesmen_pendidikan is None:
        flash("Fitur asesmen belum aktif: DATABASE_URL/pg_data belum siap.", "danger")
        return redirect(url_for("map_view"))

    # Upload foto (opsional, maksimal 2)
    try:
        photo_path = _get_asesmen_photo_path('pendidikan')
    except Exception as e:
        flash(f"Gagal upload foto asesmen pendidikan: {e}", "danger")
        return redirect(url_for("map_view"))


    kode_posko = request.form.get("kode_posko") or None
    lat = request.form.get("latitude")
    lon = request.form.get("longitude")
    catatan = request.form.get("catatan") or None
    radius_in = request.form.get("radius")
    waktu_utc = _parse_waktu_form(request.form.get("waktu"))
    try:
        radius = float(radius_in) if radius_in not in (None, "") else 2.0
    except Exception:
        radius = 2.0
    if radius <= 0:
        radius = 2.0
    if radius > 50:
        radius = 50.0

    # 10 soal (skala 1-5)
    answers = {f"p{i}": _ask_int_1_5(request.form.get(f"p{i}")) for i in range(1, 11)}

    weights = {
        "p1": 1.4,
        "p2": 1.0,
        "p3": 1.0,
        "p4": 1.0,
        "p5": 0.8,
        "p6": 1.5,
        "p7": 0.9,
        "p8": 1.3,
        "p9": 0.8,
        "p10": 0.9,
    }

    weighted_sum = sum(answers[k] * weights[k] for k in answers)
    max_sum = sum(5 * weights[k] for k in answers)

    skor_100 = (weighted_sum / max_sum) * 100.0

    if skor_100 >= 80:
        status = "Kritis"
    elif skor_100 >= 60:
        status = "Waspada"
    else:
        status = "Aman"

    try:
        pg_insert_asesmen_pendidikan(
            id_relawan=session.get("id_relawan", "UNKNOWN"),
            kode_posko=kode_posko,
            jawaban=answers,
            skor=float(skor_100),
            status=status,
            latitude=lat,
            longitude=lon,
            catatan=catatan,
            photo_path=photo_path,
            radius=radius,
            waktu=waktu_utc
        )
        flash(f"Asesmen Pendidikan tersimpan (Status: {status}, Skor: {skor_100:.1f}).", "success")
    except Exception as e:
        flash(f"Gagal simpan asesmen pendidikan: {e}", "danger")

    return redirect(url_for("map_view"))

@app.route("/submit_asesmen_psikososial", methods=["POST"])
def submit_asesmen_psikososial():
    if not _require_login():
        return redirect(url_for("map_view"))

    if not _pg_enabled() or pg_insert_asesmen_psikososial is None:
        flash("Fitur asesmen belum aktif: DATABASE_URL/pg_data belum siap.", "danger")
        return redirect(url_for("map_view"))

    # Upload foto (opsional, maksimal 2)
    try:
        photo_path = _get_asesmen_photo_path('psikososial')
    except Exception as e:
        flash(f"Gagal upload foto asesmen psikososial: {e}", "danger")
        return redirect(url_for("map_view"))


    kode_posko = request.form.get("kode_posko") or None
    lat = request.form.get("latitude")
    lon = request.form.get("longitude")
    catatan = request.form.get("catatan") or None
    radius_in = request.form.get("radius")
    waktu_utc = _parse_waktu_form(request.form.get("waktu"))

    try:
        radius = float(radius_in) if radius_in not in (None, "") else 2.0
    except Exception:
        radius = 2.0
    if radius <= 0:
        radius = 2.0
    if radius > 50:
        radius = 50.0

    # 10 soal (skala 1-5)
    answers = {f"p{i}": _ask_int_1_5(request.form.get(f"p{i}")) for i in range(1, 11)}

    weights = {
        "p1": 1.4,
        "p2": 1.0,
        "p3": 1.0,
        "p4": 1.0,
        "p5": 0.8,
        "p6": 1.5,
        "p7": 0.9,
        "p8": 1.3,
        "p9": 0.8,
        "p10": 0.9,
    }

    weighted_sum = sum(answers[k] * weights[k] for k in answers)
    max_sum = sum(5 * weights[k] for k in answers)

    skor_100 = (weighted_sum / max_sum) * 100.0

    if skor_100 >= 80:
        status = "Kritis"
    elif skor_100 >= 60:
        status = "Waspada"
    else:
        status = "Aman"

    try:
        pg_insert_asesmen_psikososial(
            id_relawan=session.get("id_relawan", "UNKNOWN"),
            kode_posko=kode_posko,
            jawaban=answers,
            skor=float(skor_100),
            status=status,
            latitude=lat,
            longitude=lon,
            catatan=catatan,
            photo_path=photo_path,
            radius=radius,
            waktu=waktu_utc
        )
        flash(f"Asesmen Psikososial tersimpan (Status: {status}, Skor: {skor_100:.1f}).", "success")
    except Exception as e:
        flash(f"Gagal simpan asesmen psikososial: {e}", "danger")

    return redirect(url_for("map_view"))

@app.route("/submit_asesmen_infrastruktur", methods=["POST"])
def submit_asesmen_infrastruktur():
    if not _require_login():
        return redirect(url_for("map_view"))

    if not _pg_enabled() or pg_insert_asesmen_infrastruktur is None:
        flash("Fitur asesmen belum aktif: DATABASE_URL/pg_data belum siap.", "danger")
        return redirect(url_for("map_view"))

    # Upload foto (opsional, maksimal 2)
    try:
        photo_path = _get_asesmen_photo_path('infrastruktur')
    except Exception as e:
        flash(f"Gagal upload foto asesmen infrastruktur: {e}", "danger")
        return redirect(url_for("map_view"))


    kode_posko = request.form.get("kode_posko") or None
    lat = request.form.get("latitude")
    lon = request.form.get("longitude")
    catatan = request.form.get("catatan") or None
    radius_in = request.form.get("radius")
    waktu_utc = _parse_waktu_form(request.form.get("waktu"))

    try:
        radius = float(radius_in) if radius_in not in (None, "") else 2.0
    except Exception:
        radius = 2.0
    if radius <= 0:
        radius = 2.0
    if radius > 50:
        radius = 50.0

    # 10 soal (skala 1-5)
    answers = {f"p{i}": _ask_int_1_5(request.form.get(f"p{i}")) for i in range(1, 11)}

    weights = {
        "p1": 1.4,
        "p2": 1.0,
        "p3": 1.0,
        "p4": 1.0,
        "p5": 0.8,
        "p6": 1.5,
        "p7": 0.9,
        "p8": 1.3,
        "p9": 0.8,
        "p10": 0.9,
    }

    weighted_sum = sum(answers[k] * weights[k] for k in answers)
    max_sum = sum(5 * weights[k] for k in answers)

    skor_100 = (weighted_sum / max_sum) * 100.0

    if skor_100 >= 80:
        status = "Kritis"
    elif skor_100 >= 60:
        status = "Waspada"
    else:
        status = "Aman"

    try:
        pg_insert_asesmen_infrastruktur(
            id_relawan=session.get("id_relawan", "UNKNOWN"),
            kode_posko=kode_posko,
            jawaban=answers,
            skor=float(skor_100),
            status=status,
            latitude=lat,
            longitude=lon,
            catatan=catatan,
            photo_path=photo_path,
            radius=radius,
            waktu=waktu_utc
        )
        flash(f"Asesmen Infrastruktur tersimpan (Status: {status}, Skor: {skor_100:.1f}).", "success")
    except Exception as e:
        flash(f"Gagal simpan asesmen infrastruktur: {e}", "danger")

    return redirect(url_for("map_view"))

@app.route("/submit_asesmen_wash", methods=["POST"])
def submit_asesmen_wash():
    if not _require_login():
        return redirect(url_for("map_view"))

    if not _pg_enabled() or pg_insert_asesmen_wash is None:
        flash("Fitur asesmen belum aktif: DATABASE_URL/pg_data belum siap.", "danger")
        return redirect(url_for("map_view"))

    # Upload foto (opsional, maksimal 2)
    try:
        photo_path = _get_asesmen_photo_path('wash')
    except Exception as e:
        flash(f"Gagal upload foto asesmen wash: {e}", "danger")
        return redirect(url_for("map_view"))


    kode_posko = request.form.get("kode_posko") or None
    lat = request.form.get("latitude")
    lon = request.form.get("longitude")
    catatan = request.form.get("catatan") or None
    radius_in = request.form.get("radius")
    waktu_utc = _parse_waktu_form(request.form.get("waktu"))

    try:
        radius = float(radius_in) if radius_in not in (None, "") else 2.0
    except Exception:
        radius = 2.0
    if radius <= 0:
        radius = 2.0
    if radius > 50:
        radius = 50.0

    # 10 soal (skala 1-5)
    answers = {f"p{i}": _ask_int_1_5(request.form.get(f"p{i}")) for i in range(1, 11)}

    weights = {
        "p1": 1.4,
        "p2": 1.0,
        "p3": 1.0,
        "p4": 1.0,
        "p5": 0.8,
        "p6": 1.5,
        "p7": 0.9,
        "p8": 1.3,
        "p9": 0.8,
        "p10": 0.9,
    }

    weighted_sum = sum(answers[k] * weights[k] for k in answers)
    max_sum = sum(5 * weights[k] for k in answers)

    skor_100 = (weighted_sum / max_sum) * 100.0

    if skor_100 >= 80:
        status = "Kritis"
    elif skor_100 >= 60:
        status = "Waspada"
    else:
        status = "Aman"

    try:
        pg_insert_asesmen_wash(
            id_relawan=session.get("id_relawan", "UNKNOWN"),
            kode_posko=kode_posko,
            jawaban=answers,
            skor=float(skor_100),
            status=status,
            latitude=lat,
            longitude=lon,
            catatan=catatan,
            photo_path=photo_path,
            radius=radius,
            waktu=waktu_utc,
        )
        flash(f"Asesmen Wash tersimpan (Status: {status}, Skor: {skor_100:.1f}).", "success")
    except Exception as e:
        flash(f"Gagal simpan asesmen wash: {e}", "danger")
        print(e)

    return redirect(url_for("map_view"))

@app.route("/submit_asesmen_kondisi", methods=["POST"])
def submit_asesmen_kondisi():
    if not _require_login():
        return redirect(url_for("map_view"))

    try:
        if not _pg_enabled() or pg_insert_asesmen_kondisi is None:
            flash("Fitur asesmen belum aktif.", "danger")
            return redirect(url_for("map_view"))

        # Upload foto (opsional, maksimal 2)
        try:
            photo_path = _get_asesmen_photo_path('kondisi')
        except Exception as e:
            flash(f"Gagal upload foto asesmen kondisi: {e}", "danger")
            return redirect(url_for("map_view"))


        kode_posko = request.form.get("kode_posko")
        lat = request.form.get("latitude")
        lon = request.form.get("longitude")
        catatan = request.form.get("catatan")
        radius = float(request.form.get("radius", 2) or 2)
        waktu_utc = _parse_waktu_form(request.form.get("waktu"))

        # ===== Kondisi Banjir =====
        lokasi = request.form.get("lokasi")
        lat_list = request.form.getlist("banjir_lat[]")
        lon_list = request.form.getlist("banjir_lon[]")
        p2 = {}
        for i, (lat, lon) in enumerate(zip(lat_list, lon_list), start=1):
            p2[f"b{i}"] = [float(lat), float(lon)]

        payload = {
            "p1": lokasi,
            "p2": p2,
            **{
                f"p{i}": int(request.form.get(f"k{i-2}", 0))
                for i in range(3, 12)
            }
        }


        # ===== HITUNG SKOR (NANTI) =====
        skor_100 = 0

        if skor_100 >= 80:
            status = "Kritis"
        elif skor_100 >= 60:
            status = "Waspada"
        else:
            status = "Aman"

        # ===== SIMPAN =====
        pg_insert_asesmen_kondisi(
            id_relawan=session.get("id_relawan"),
            kode_posko=kode_posko,
            jawaban=payload,
            skor=skor_100,
            status=status,
            latitude=lat,
            longitude=lon,
            catatan=catatan,
            photo_path=photo_path,
            radius=radius,
            waktu=waktu_utc
        )

        flash("Asesmen Kondisi berhasil disimpan", "success")
        return redirect(url_for("map_view"))

    except Exception as e:
        import traceback
        traceback.print_exc()  # ⬅️ WAJIB UNTUK DEBUG
        flash(f"Error asesmen kondisi: {e}", "danger")
        return redirect(url_for("map_view"))

@app.route("/submit_lokasi", methods=["POST"])
def submit_lokasi():
    if not _require_login():
        return redirect(url_for("map_view"))

    if not _pg_enabled() or pg_insert_data_lokasi is None:
        flash("Fitur input lokasi belum aktif: DATABASE_URL/pg_data belum siap.", "danger")
        return redirect(url_for("map_view"))

    # ID diset oleh sistem (relawan tidak perlu input)
    id_lokasi = None

    jenis_lokasi = (request.form.get("jenis_lokasi") or "").strip()
    nama_kabkota = (request.form.get("nama_kabkota") or "").strip()
    nama_lokasi = (request.form.get("nama_lokasi") or "").strip()

    # defaults kalau kosong
    status_lokasi = (request.form.get("status_lokasi") or "").strip() or "Aktif"
    tingkat_akses = (request.form.get("tingkat_akses") or "").strip() or "Public"
    kondisi = (request.form.get("kondisi") or "").strip() or "Normal"

    alamat = (request.form.get("alamat") or "").strip() or None
    kecamatan = (request.form.get("kecamatan") or "").strip() or None
    desa_kelurahan = (request.form.get("desa_kelurahan") or "").strip() or None

    latitude = (request.form.get("latitude") or "").strip()
    longitude = (request.form.get("longitude") or "").strip()
    waktu_utc = _parse_waktu_form(request.form.get("waktu"))

    if not latitude or not longitude:
        flash("Gagal simpan lokasi: koordinat GPS belum didapat. Coba tunggu GPS OK / geser pin.", "danger")
        return redirect(url_for("map_view"))

    lokasi_text = (request.form.get("lokasi_text") or "").strip() or None
    catatan = (request.form.get("catatan") or "").strip() or None

    pic = (request.form.get("pic") or "").strip() or None
    pic_hp = (request.form.get("pic_hp") or "").strip() or None
    photo_path = (request.form.get("photo_path") or "").strip() or None
    photo_file = request.files.get("photo_lokasi")

    if not jenis_lokasi or not nama_kabkota or not nama_lokasi:
        flash("Gagal simpan lokasi: Jenis Lokasi, Kab/Kota, dan Nama Lokasi wajib diisi.", "danger")
        return redirect(url_for("map_view"))

    try:
        new_id = pg_insert_data_lokasi(
            id_lokasi=id_lokasi,
            id_relawan=session.get("id_relawan"),  # ✅ TAMBAH
            jenis_lokasi=jenis_lokasi,
            nama_kabkota=nama_kabkota,
            status_lokasi=status_lokasi,
            tingkat_akses=tingkat_akses,
            kondisi=kondisi,
            nama_lokasi=nama_lokasi,
            alamat=alamat,
            kecamatan=kecamatan,
            desa_kelurahan=desa_kelurahan,
            latitude=latitude,
            longitude=longitude,
            lokasi_text=lokasi_text,
            catatan=catatan,
            pic=pic,
            pic_hp=pic_hp,
            photo_path=photo_path,
            waktu=waktu_utc,  # ✅ TAMBAH
        )
        flash(f"Lokasi berhasil disimpan: {new_id}", "success")
    except Exception as e:
        flash(f"Gagal simpan lokasi: {e}", "danger")

    # Upload foto lokasi (opsional) -> media/photo_lokasi/<ID_LOKASI>.<ext>
    try:
        if photo_file and getattr(photo_file, "filename", ""):
            saved = save_lokasi_photo(photo_file, id_lokasi=new_id, media_root=MEDIA_DIR)
            if saved and pg_update_data_lokasi_photo_path is not None:
                pg_update_data_lokasi_photo_path(new_id, saved)
    except Exception as e:
        flash(f"Lokasi tersimpan, tapi upload foto gagal: {e}", "warning")

    return redirect(url_for("map_view"))

if __name__ == "__main__":
    app.run(debug=True)
