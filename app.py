from flask import Flask, render_template, request, redirect, url_for, session
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import datetime
import os # Untuk mendapatkan waktu saat ini dan Secret Key

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
# ROUTE UTAMA
# ==============================================================================

@app.route("/")
def map_view():
    data_lokasi_raw = get_sheet_data("data_lokasi")
    data_lokasi = [d for d in data_lokasi_raw if d.get('latitude') and d.get('longitude')]

    stok_gudang = get_sheet_data("stok_gudang")

    rekap_kabkota = get_sheet_data("rekapitulasi_data_kabkota")
    latest_rekap = {}
    for row in rekap_kabkota:
        kabkota = row.get('kabkota')
        if kabkota and kabkota not in latest_rekap:
            latest_rekap[kabkota] = row

    data_relawan = get_sheet_data("data_relawan")
    
    # Ambil daftar posko untuk form permintaan
    data_posko = [d.get('kode_lokasi') for d in data_lokasi if d.get('jenis_lokasi') == 'Posko Pengungsian']
    data_barang = [d.get('kode_barang') for d in get_sheet_data("master_logistik")]

    return render_template("map.html", 
                           data_lokasi=json.dumps(data_lokasi), 
                           stok_gudang=stok_gudang, 
                           rekap_kabkota=list(latest_rekap.values()),
                           relawan_list=data_relawan,
                           data_posko=data_posko,
                           data_barang=data_barang,
                           logged_in=session.get('logged_in', False),
                           nama_relawan=session.get('nama_relawan', ''))

# ==============================================================================
# 1. LOGIKA LOGIN
# ==============================================================================
@app.route("/login", methods=["POST"])
def login():
    nama = request.form.get('nama')
    kode_akses = request.form.get('kode_akses')
    
    # Pengecekan KODE AKSES yang fix: "usupeduli"
    if kode_akses != "usupeduli":
        print("Login gagal: Kode akses salah.")
        return redirect(url_for('map_view'))

    relawan_list = get_sheet_data("data_relawan")
    valid_login = False
    
    for r in relawan_list:
        if r.get('nama_relawan', '').lower() == nama.lower():
            valid_login = True
            session['logged_in'] = True
            session['nama_relawan'] = r['nama_relawan']
            session['id_relawan'] = r.get('id_relawan', 'UNKNOWN')
            break

    if not valid_login:
        print(f"Login gagal: Nama relawan '{nama}' tidak ditemukan.")
    
    return redirect(url_for('map_view'))

@app.route("/logout", methods=["POST"])
def logout():
    session.pop('logged_in', None)
    session.pop('nama_relawan', None)
    session.pop('id_relawan', None)
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

    append_to_sheet("permintaan_posko", data)
    
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