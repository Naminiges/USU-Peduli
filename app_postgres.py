from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import datetime
import os # Untuk mendapatkan waktu saat ini dan Secret Key
import logging
import math
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------------------
# OPTIONAL: PostgreSQL helper (fallback/primary) - TIDAK mengubah logic lama
# ------------------------------------------------------------------------------
try:
    from pg_data import (
        pg_get_status_map,
        pg_get_data_lokasi,
        pg_get_relawan_list,
        pg_insert_lokasi_relawan,
        pg_insert_asesmen_kesehatan,
        pg_insert_asesmen_pendidikan,
        ensure_kabkota_geojson_static,
    )
except Exception as _pg_err:
    pg_get_status_map = None
    pg_get_data_lokasi = None
    pg_get_relawan_list = None
    pg_insert_lokasi_relawan = None
    pg_insert_asesmen_kesehatan = None
    pg_insert_asesmen_pendidikan = None
    ensure_kabkota_geojson_static = None


app = Flask(__name__)
# HARUS diganti dengan kunci rahasia yang kuat untuk mengamankan sesi
app.secret_key = os.environ.get('SECRET_KEY')
if not app.secret_key:
    raise RuntimeError("SECRET_KEY belum diset di environment")

# Konfigurasi Google Sheets API (Asumsi file service_account.json sudah benar)
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

try:
    service_account_json = os.environ.get('SERVICE_ACCOUNT_JSON')
    if not service_account_json:
        raise RuntimeError("SERVICE_ACCOUNT_JSON belum diset")

    creds_dict = json.loads(service_account_json)

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        creds_dict, scope
    )
    client = gspread.authorize(creds)

except Exception as e:
    print(f"Error otorisasi Google Sheets: {e}")
    client = None


# Spreadsheet ID (berdasarkan file yang Anda unggah)
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
if not SPREADSHEET_ID:
    raise RuntimeError("SPREADSHEET_ID belum diset di environment")


def get_sheet_data(sheet_name):
    """Fungsi pembantu untuk mengambil data dari sheet tertentu."""
    if not client: return []
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        return sheet.get_all_records()
    except Exception as e:
        print(f"Error reading sheet {sheet_name}: {e}")
        return []

def _pg_enabled() -> bool:
    return bool(os.environ.get("DATABASE_URL"))


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
    """
    Prefer Google Sheets (lama). Kalau kosong/gagal -> fallback ke Postgres.
    Output: {KABKOTA_UPPER: status}
    """
    # 1) Sheets
    status_map = {}
    try:
        data_status = get_sheet_data("status_bencana_kabkota")
        for row in data_status:
            nama_kota = row.get('kabkota')
            status = row.get('status_bencana')
            if nama_kota and status:
                status_map[nama_kota.strip().upper()] = str(status).strip()
    except Exception as e:
        print(f"[Sheets] get_status_map_any error: {e}")

    if status_map:
        return status_map

    # 2) Postgres
    if _pg_enabled() and pg_get_status_map is not None:
        try:
            return pg_get_status_map() or {}
        except Exception as e:
            print(f"[PG] get_status_map_any error: {e}")

    return {}


def get_data_lokasi_any() -> list:
    """
    Prefer Google Sheets (lama). Kalau kosong/gagal -> fallback ke Postgres.
    Output: list dict lokasi (RAW, bisa ada yang belum punya koordinat).
    """
    # 1) Sheets (raw)
    try:
        data_lokasi_raw = get_sheet_data("data_lokasi")
        if data_lokasi_raw:
            return data_lokasi_raw
    except Exception as e:
        print(f"[Sheets] get_data_lokasi_any error: {e}")

    # 2) Postgres
    if _pg_enabled() and pg_get_data_lokasi is not None:
        try:
            # Postgres versi awal: kalau ada baris tanpa koordinat, tetap boleh (tidak difilter di sini).
            return pg_get_data_lokasi() or []
        except Exception as e:
            print(f"[PG] get_data_lokasi_any error: {e}")

    return []


def get_relawan_list_any() -> list:
    """Prefer Sheets -> fallback Postgres."""
    relawan_list = []
    try:
        relawan_list = get_relawan_list_any()
    except Exception as e:
        print(f"[Sheets] get_relawan_list_any error: {e}")

    if relawan_list:
        return relawan_list

    if _pg_enabled() and pg_get_relawan_list is not None:
        try:
            return pg_get_relawan_list() or []
        except Exception as e:
            print(f"[PG] get_relawan_list_any error: {e}")

    return []


def write_lokasi_relawan_any(data: dict) -> bool:
    """
    Simpan absensi:
    - Prefer Postgres (lokasi_relawan)
    - Kalau gagal -> coba append ke Sheets (lokasi_relawan)
    """
    # Prefer Postgres
    if _pg_enabled() and pg_insert_lokasi_relawan is not None:
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
                )
            )
        except Exception as e:
            print(f"[PG] write_lokasi_relawan_any error: {e}")

    # Fallback Sheets
    try:
        return bool(append_to_sheet("lokasi_relawan", data))
    except Exception as e:
        print(f"[Sheets] write_lokasi_relawan_any error: {e}")
        return False


def get_next_id(sheet_name, prefix):
    """Fungsi pembantu untuk mendapatkan ID berikutnya (misal R0001, L0001)."""
    if not client: return f"{prefix}0000"
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        all_ids = sheet.col_values(1)[1:] 
        last_id = [i for i in all_ids if i] 
        if not last_id:
            return f"{prefix}0001"
        
        last_id = last_id[-1]
        last_number = int(last_id[len(prefix):])
        new_number = last_number + 1
        return f"{prefix}{new_number:04d}" 
    except Exception as e:
        print(f"Error getting next ID for {sheet_name}: {e}")
        return f"{prefix}{datetime.datetime.now().strftime('%m%d%H%M')}"

def append_to_sheet(sheet_name, data):
    """Fungsi pembantu untuk menambahkan baris data ke sheet, menyesuaikan urutan header."""
    if not client: return False
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        headers = sheet.row_values(1)
        row_values = [data.get(header, '') for header in headers]
        sheet.append_row(row_values)
        return True
    except Exception as e:
        print(f"Error appending to sheet {sheet_name}: {e}")
        return False

def sanitize_for_log(text):
    """Sanitasi teks untuk mencegah injection dan karakter berbahaya di log."""
    if not text:
        return ""
    # Hapus karakter kontrol dan newline yang tidak aman
    text = str(text).replace('\n', ' ').replace('\r', ' ')
    # Hapus karakter kontrol lainnya
    text = ''.join(char for char in text if ord(char) >= 32 or char in '\t')
    # Batasi panjang untuk mencegah log terlalu besar
    return text[:500]

def get_log_directory():
    """Mendapatkan direktori log yang aman (di luar web root)."""
    # Buat folder logs di root project (sibling dengan app.py)
    log_dir = Path(__file__).parent / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    # Buat file .htaccess untuk Apache (jika menggunakan Apache)
    htaccess_file = log_dir / '.htaccess'
    if not htaccess_file.exists():
        try:
            with open(htaccess_file, 'w', encoding='utf-8') as f:
                f.write("# Deny access to log files\n")
                f.write("Order deny,allow\n")
                f.write("Deny from all\n")
        except Exception:
            pass  # Jika gagal (misalnya bukan Apache), abaikan
    
    # Buat file web.config untuk IIS (jika menggunakan IIS)
    webconfig_file = log_dir / 'web.config'
    if not webconfig_file.exists():
        try:
            with open(webconfig_file, 'w', encoding='utf-8') as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                f.write('<configuration>\n')
                f.write('  <system.webServer>\n')
                f.write('    <authorization>\n')
                f.write('      <deny users="*" />\n')
                f.write('    </authorization>\n')
                f.write('  </system.webServer>\n')
                f.write('</configuration>\n')
        except Exception:
            pass  # Jika gagal, abaikan
    
    # Buat file README untuk dokumentasi
    readme_file = log_dir / 'README.txt'
    if not readme_file.exists():
        try:
            with open(readme_file, 'w', encoding='utf-8') as f:
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
    """
    Log permintaan posko ke file log.txt dengan format yang aman.
    
    Args:
        data_permintaan: Dictionary berisi data permintaan
        nama_relawan: Nama relawan yang meminta
        id_relawan: ID relawan
        nama_posko: Nama posko (opsional, akan dicari jika tidak ada)
    """
    try:
        log_dir = get_log_directory()
        log_file = log_dir / 'log.txt'
        
        # Sanitasi semua input
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        nama_relawan_safe = sanitize_for_log(nama_relawan)
        id_relawan_safe = sanitize_for_log(id_relawan)
        id_permintaan = sanitize_for_log(data_permintaan.get('id_permintaan', 'UNKNOWN'))
        kode_posko = sanitize_for_log(data_permintaan.get('kode_posko', 'UNKNOWN'))
        nama_posko_safe = sanitize_for_log(nama_posko) if nama_posko else kode_posko
        kode_barang = sanitize_for_log(data_permintaan.get('kode_barang', 'UNKNOWN'))
        jumlah_diminta = sanitize_for_log(data_permintaan.get('jumlah_diminta', '0'))
        keterangan = sanitize_for_log(data_permintaan.get('keterangan', ''))
        status = sanitize_for_log(data_permintaan.get('status', 'Draft'))
        tanggal = sanitize_for_log(data_permintaan.get('tanggal', ''))
        
        # Format log yang terstruktur dan mudah dibaca
        log_entry = (
            f"[{timestamp}] PERMINTAAN_POSKO | "
            f"ID: {id_permintaan} | "
            f"Peminta: {nama_relawan_safe} (ID: {id_relawan_safe}) | "
            f"Posko: {nama_posko_safe} ({kode_posko}) | "
            f"Barang: {kode_barang} | "
            f"Jumlah: {jumlah_diminta} | "
            f"Status: {status} | "
            f"Tanggal: {tanggal} | "
            f"Keterangan: {keterangan if keterangan else '(tidak ada)'}"
        )
        
        # Tulis ke file dengan mode append
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
            
    except Exception as e:
        # Jangan crash aplikasi jika logging gagal, cukup print error
        print(f"Error writing to log file: {e}")

# ==============================================================================
# KEAMANAN: Blokir akses ke folder logs melalui URL
# ==============================================================================
@app.route('/logs/<path:filename>')
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
        # Ambil data terbaru dari Google Sheets
        data_lokasi_raw = get_data_lokasi_any()
        data_lokasi = [d for d in data_lokasi_raw if d.get('latitude') and d.get('longitude')]
        
        # Ambil status bencana terbaru
        status_map = get_status_map_any()
        
        return jsonify({
            'success': True,
            'data_lokasi': data_lokasi,
            'status_map': status_map
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# ==============================================================================
# ROUTE UTAMA
# ==============================================================================

@app.route("/")
def map_view():
    data_lokasi_raw = get_data_lokasi_any()
    data_lokasi = [d for d in data_lokasi_raw if d.get('latitude') and d.get('longitude')]

    stok_gudang = get_sheet_data("stok_gudang")

    status_map = get_status_map_any()

    rekap_kabkota = get_sheet_data("rekapitulasi_data_kabkota")
    latest_rekap = {}
    for row in rekap_kabkota:
        kabkota = row.get('kabkota')
        if kabkota and kabkota not in latest_rekap:
            latest_rekap[kabkota] = row

    data_relawan = get_relawan_list_any()
    
    # Ambil daftar posko untuk form permintaan dengan nama dan kode
    data_posko_list = []
    for d in data_lokasi:
        if d.get('jenis_lokasi') == 'Posko Pengungsian':
            kode = d.get('kode_lokasi')
            nama = d.get('nama_lokasi') or d.get('kabupaten_kota') or kode
            data_posko_list.append({'kode': kode, 'nama': nama})
    
    data_barang = [d.get('kode_barang') for d in get_sheet_data("master_logistik")]

    return render_template("map.html", 
                           data_lokasi=json.dumps(data_lokasi), 
                           stok_gudang=stok_gudang, 
                           rekap_kabkota=list(latest_rekap.values()),
                           relawan_list=data_relawan,
                           data_posko=data_posko_list,
                           data_barang=data_barang,
                           logged_in=session.get('logged_in', False),
                           nama_relawan=session.get('nama_relawan', ''),
                           status_map=json.dumps(status_map))

# ==============================================================================
# 1. LOGIKA LOGIN
# ==============================================================================
@app.route("/login", methods=["POST"])
def login():
    nama = request.form.get('nama')
    kode_akses = request.form.get('kode_akses')
    # Basic validation
    if not nama or not kode_akses:
        flash("Login gagal: Nama dan Kode Akses harus diisi.", "danger")
        return redirect(url_for('map_view'))

    relawan_list = get_relawan_list_any()

    # Cari relawan sesuai nama (case-insensitive)
    matched = None
    for r in relawan_list:
        if r.get('nama_relawan', '').strip().lower() == (nama or '').strip().lower():
            matched = r
            break

    if not matched:
        flash(f"Login gagal: Nama relawan '{nama}' tidak ditemukan di database.", "danger")
        return redirect(url_for('map_view'))

    expected_code = (matched.get('kode_akses') or '').strip()
    if not expected_code:
        flash("Login gagal: Relawan belum memiliki kode akses terdaftar. Hubungi admin.", "danger")
        return redirect(url_for('map_view'))

    # Cocokkan kode akses sesuai relawan (case-insensitive)
    if kode_akses.strip().lower() != expected_code.lower():
        flash("Login gagal: Kode akses salah.", "danger")
        return redirect(url_for('map_view'))

    # Login berhasil
    session['logged_in'] = True
    session['nama_relawan'] = matched['nama_relawan']
    session['id_relawan'] = matched.get('id_relawan', 'UNKNOWN')
    flash(f"Login berhasil! Selamat bertugas, {matched['nama_relawan']}.", "success")
    return redirect(url_for('map_view'))

@app.route("/logout", methods=["POST"])
def logout():
    session.pop('logged_in', None)
    session.pop('nama_relawan', None)
    session.pop('id_relawan', None)
    flash("Logout Berhasil.", "success")
    return redirect(url_for('map_view'))

# ==============================================================================
# 2. LOGIKA PERMINTAAN POSKO
# ==============================================================================
@app.route("/submit_permintaan", methods=["POST"])
def submit_permintaan():
    if not session.get('logged_in'):
        return redirect(url_for('map_view'))

    kode_posko = request.form.get('kode_posko')
    kode_barang = request.form.get('kode_barang')
    jumlah_diminta = request.form.get('jumlah_diminta')
    
    # Ambil nama posko untuk log yang lebih informatif
    nama_posko = None
    data_lokasi_raw = get_data_lokasi_any()
    for lokasi in data_lokasi_raw:
        if lokasi.get('kode_lokasi') == kode_posko:
            nama_posko = lokasi.get('nama_lokasi') or lokasi.get('kabupaten_kota') or kode_posko
            break
    
    data = {
        'id_permintaan': get_next_id("permintaan_posko", "R"),
        'tanggal': datetime.datetime.now().strftime('%d-%B-%y'),
        'kode_posko': kode_posko,
        'kode_barang': kode_barang,
        'jumlah_diminta': jumlah_diminta,
        'status': 'Draft', 
        'keterangan': request.form.get('keterangan', ''),
        'relawan': session.get('id_relawan', session.get('nama_relawan')), 
        'photo_link': ''
    }

    # Simpan ke Google Sheets
    append_to_sheet("permintaan_posko", data)
    
    # Log permintaan ke file log.txt
    nama_relawan = session.get('nama_relawan', 'UNKNOWN')
    id_relawan = session.get('id_relawan', 'UNKNOWN')
    log_permintaan_posko(data, nama_relawan, id_relawan, nama_posko)
    flash(f"Permintaan {nama_posko} berhasil dikirim!", "success")
    return redirect(url_for('map_view'))

# ==============================================================================
# 3. LOGIKA ABSENSI RELAWAN
# ==============================================================================
def point_in_polygon(point, polygon):
    """
    Mengecek apakah titik (lon, lat) ada di dalam list koordinat polygon.
    Algoritma: Ray Casting.
    """
    x, y = point # x = longitude, y = latitude
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
    """
    Looping semua wilayah di GeoJSON Sumut untuk mencari user ada di mana.
    """
    try:
        json_path = os.path.join(app.root_path, 'static', 'data', 'kabkota_sumut.json')
        
        with open(json_path, 'r') as f:
            data = json.load(f)
            
        point = (float(user_lon), float(user_lat))
        
        for feature in data['features']:
            geometry = feature['geometry']
            props = feature['properties']
            
            # Ambil nama kota (sesuaikan dengan nama kolom di GeoJSON kamu, misal 'kabkota')
            nama_wilayah = props.get('kabkota') or props.get('KABKOTA') or props.get('NAMOBJ') or "Wilayah Tak Bernama"
            
            found = False
            
            # GeoJSON bisa berupa 'Polygon' (1 pulau) atau 'MultiPolygon' (banyak pulau)
            if geometry['type'] == 'Polygon':
                # Polygon biasa: koordinat ada di geometry['coordinates'][0]
                poly_coords = geometry['coordinates'][0] 
                if point_in_polygon(point, poly_coords):
                    found = True
                    
            elif geometry['type'] == 'MultiPolygon':
                # MultiPolygon: Loop setiap pulau/bagian
                for poly in geometry['coordinates']:
                    poly_coords = poly[0]
                    if point_in_polygon(point, poly_coords):
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
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return 6371.0 * c
    except Exception:
        return float('inf')
    
@app.route("/submit_absensi", methods=["POST"])
def submit_absensi():
    if not session.get('logged_in'):
        flash(f"Silahkan login terlebih dahulu!", "danger")
        return redirect(url_for('map_view'))

    latitude = request.form.get('latitude')
    longitude = request.form.get('longitude')
    catatan = request.form.get('catatan')

    lokasi_terdeteksi = "Mencari..."

    if latitude and longitude:
        lokasi_terdeteksi = cek_wilayah_geojson(latitude, longitude)
    
    # Cari posko terdekat berdasarkan koordinat absensi
    lokasi_posko_code = ''
    try:
        data_lokasi_raw = get_data_lokasi_any()
        # Prioritaskan Posko Pengungsian; jika tidak ada, gunakan semua lokasi dengan koordinat
        candidates = [l for l in data_lokasi_raw if l.get('jenis_lokasi') == 'Posko Pengungsian' and l.get('latitude') and l.get('longitude')]
        if not candidates:
            candidates = [l for l in data_lokasi_raw if l.get('latitude') and l.get('longitude')]

        min_dist = None
        nearest = None
        for p in candidates:
            try:
                plat = float(p.get('latitude'))
                plong = float(p.get('longitude'))
                dist = haversine_distance(latitude, longitude, plat, plong)
                if min_dist is None or dist < min_dist:
                    min_dist = dist
                    nearest = p
            except Exception:
                continue

        if nearest:
            lokasi_posko_code = nearest.get('kode_lokasi') or nearest.get('kode') or ''
    except Exception as e:
        print(f"Error mencari posko terdekat: {e}")
    
    data = {
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'id_relawan': session.get('id_relawan', 'UNKNOWN'),
        'latitude': latitude,
        'longitude': longitude,
        'lokasi': lokasi_terdeteksi, 
        'lokasi_posko': lokasi_posko_code,
        'catatan': catatan,
        'photo_link': ''
    }

    write_lokasi_relawan_any(data)
    msg_type = "success" if lokasi_terdeteksi != "Luar Wilayah Sumut" else "warning"
    flash(f"Absensi berhasil! Posisi Anda terdeteksi di: {lokasi_terdeteksi}", msg_type)
    return redirect(url_for('map_view'))


# ==============================================================================
# 4. ASESMEN (KESEHATAN & PENDIDIKAN) -> POSTGRES
# ==============================================================================
def _ask_int_1_5(val, default: int = 1) -> int:
    try:
        v = int(val)
        if v < 1: v = 1
        if v > 5: v = 5
        return v
    except Exception:
        return default


def _require_login():
    if not session.get('logged_in'):
        flash("Silahkan login terlebih dahulu!", "danger")
        return False
    return True


@app.route("/submit_asesmen_kesehatan", methods=["POST"])
def submit_asesmen_kesehatan():
    if not _require_login():
        return redirect(url_for('map_view'))

    if not _pg_enabled() or pg_insert_asesmen_kesehatan is None:
        flash("Fitur asesmen belum aktif: DATABASE_URL/pg_data belum siap.", "danger")
        return redirect(url_for('map_view'))

    # Form data
    kode_posko = request.form.get("kode_posko") or None
    lat = request.form.get("latitude")
    lon = request.form.get("longitude")
    catatan = request.form.get("catatan") or None

    p1 = _ask_int_1_5(request.form.get("p1"))
    p2 = _ask_int_1_5(request.form.get("p2"))
    p3 = _ask_int_1_5(request.form.get("p3"))
    p4 = _ask_int_1_5(request.form.get("p4"))
    p5 = _ask_int_1_5(request.form.get("p5"))

    weights = {"p1": 1.0, "p2": 1.0, "p3": 1.0, "p4": 1.5, "p5": 1.5}
    answers = {"p1": p1, "p2": p2, "p3": p3, "p4": p4, "p5": p5}

    skor = (
        answers["p1"] * weights["p1"] +
        answers["p2"] * weights["p2"] +
        answers["p3"] * weights["p3"] +
        answers["p4"] * weights["p4"] +
        answers["p5"] * weights["p5"]
    )

    max_skor = 5*weights["p1"] + 5*weights["p2"] + 5*weights["p3"] + 5*weights["p4"] + 5*weights["p5"]
    skor_100 = (skor / max_skor) * 100.0

    if skor_100 >= 80:
        status = "Kritis"
    elif skor_100 >= 60:
        status = "Waspada"
    else:
        status = "Aman"

    try:
        pg_insert_asesmen_kesehatan(
            id_relawan=session.get('id_relawan', 'UNKNOWN'),
            kode_posko=kode_posko,
            jawaban=answers,
            skor=float(skor_100),
            status=status,
            latitude=lat,
            longitude=lon,
            catatan=catatan,
        )
        flash(f"Asesmen Kesehatan tersimpan (Status: {status}, Skor: {skor_100:.1f}).", "success")
    except Exception as e:
        flash(f"Gagal simpan asesmen kesehatan: {e}", "danger")

    return redirect(url_for('map_view'))


@app.route("/submit_asesmen_pendidikan", methods=["POST"])
def submit_asesmen_pendidikan():
    if not _require_login():
        return redirect(url_for('map_view'))

    if not _pg_enabled() or pg_insert_asesmen_pendidikan is None:
        flash("Fitur asesmen belum aktif: DATABASE_URL/pg_data belum siap.", "danger")
        return redirect(url_for('map_view'))

    kode_posko = request.form.get("kode_posko") or None
    lat = request.form.get("latitude")
    lon = request.form.get("longitude")
    catatan = request.form.get("catatan") or None

    # 10 soal (skala 1-5)
    answers = {f"p{i}": _ask_int_1_5(request.form.get(f"p{i}")) for i in range(1, 11)}

    weights = {"p1":1.4,"p2":1.0,"p3":1.0,"p4":1.0,"p5":0.8,"p6":1.5,"p7":0.9,"p8":1.3,"p9":0.8,"p10":0.9}

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
            id_relawan=session.get('id_relawan', 'UNKNOWN'),
            kode_posko=kode_posko,
            jawaban=answers,
            skor=float(skor_100),
            status=status,
            latitude=lat,
            longitude=lon,
            catatan=catatan,
        )
        flash(f"Asesmen Pendidikan tersimpan (Status: {status}, Skor: {skor_100:.1f}).", "success")
    except Exception as e:
        flash(f"Gagal simpan asesmen pendidikan: {e}", "danger")

    return redirect(url_for('map_view'))


if __name__ == "__main__":
    app.run(debug=True)