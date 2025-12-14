from flask import Flask, render_template, request, redirect, url_for, session, flash
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import datetime
import os # Untuk mendapatkan waktu saat ini dan Secret Key
import logging
from pathlib import Path

app = Flask(__name__)
# HARUS diganti dengan kunci rahasia yang kuat untuk mengamankan sesi
app.secret_key = os.environ.get('SECRET_KEY', 'usupeduli_satgas_super_rahasia_key') 

# Konfigurasi Google Sheets API (Asumsi file service_account.json sudah benar)
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

try:
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "service_account.json", scope
    )
    client = gspread.authorize(creds)
except Exception as e:
    print(f"Error otorisasi Google Sheets: {e}")
    client = None

# Spreadsheet ID (berdasarkan file yang Anda unggah)
SPREADSHEET_ID = "1jfruOftY0v5uBwx2NctrhczqYWLxmmJzhTU6pwlDRTQ" 

def get_sheet_data(sheet_name):
    """Fungsi pembantu untuk mengambil data dari sheet tertentu."""
    if not client: return []
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
        return sheet.get_all_records()
    except Exception as e:
        print(f"Error reading sheet {sheet_name}: {e}")
        return []

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

# ==============================================================================
# FUNGSI LOGGING AMAN
# ==============================================================================
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
# ROUTE UTAMA
# ==============================================================================

@app.route("/")
def map_view():
    data_lokasi_raw = get_sheet_data("data_lokasi")
    data_lokasi = [d for d in data_lokasi_raw if d.get('latitude') and d.get('longitude')]

    stok_gudang = get_sheet_data("stok_gudang")

    data_status = get_sheet_data("status_bencana_kabkota")
    status_map = {}
    for row in data_status:
        nama_kota = row.get('kabkota')
        status = row.get('status_bencana')    
        if nama_kota and status:
            # Kita buat huruf besar semua biar gampang dicocokkan
            status_map[nama_kota.strip().upper()] = status.strip()

    rekap_kabkota = get_sheet_data("rekapitulasi_data_kabkota")
    latest_rekap = {}
    for row in rekap_kabkota:
        kabkota = row.get('kabkota')
        if kabkota and kabkota not in latest_rekap:
            latest_rekap[kabkota] = row

    data_relawan = get_sheet_data("data_relawan")
    
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

    if kode_akses != "usupeduli":
        flash("Login gagal: Kode akses salah.", "danger")
        return redirect(url_for('map_view'))

    relawan_list = get_sheet_data("data_relawan")
    valid_login = False
    
    for r in relawan_list:
        if r.get('nama_relawan', '').lower() == nama.lower():
            valid_login = True
            session['logged_in'] = True
            session['nama_relawan'] = r['nama_relawan']
            session['id_relawan'] = r.get('id_relawan', 'UNKNOWN')
            flash(f"Login berhasil! Selamat bertugas, {r['nama_relawan']}.", "success")
            break

    if not valid_login:
        flash(f"Login gagal: Nama relawan '{nama}' tidak ditemukan di database.", "danger")
    
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
    data_lokasi_raw = get_sheet_data("data_lokasi")
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
    
    return redirect(url_for('map_view'))

# ==============================================================================
# 3. LOGIKA ABSENSI RELAWAN
# ==============================================================================
@app.route("/submit_absensi", methods=["POST"])
def submit_absensi():
    if not session.get('logged_in'):
        return redirect(url_for('map_view'))

    latitude = request.form.get('latitude')
    longitude = request.form.get('longitude')
    catatan = request.form.get('catatan')
    
    data = {
        'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'id_relawan': session.get('id_relawan', 'UNKNOWN'),
        'latitude': latitude,
        'longitude': longitude,
        'lokasi': f"Lat: {latitude}, Lon: {longitude}", 
        'catatan': catatan,
        'photo_link': ''
    }

    append_to_sheet("lokasi_relawan", data)

    return redirect(url_for('map_view'))

if __name__ == "__main__":
    app.run(debug=True)