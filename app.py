from flask import Flask, render_template
import gspread
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# Setup Google Sheets API
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "service_account.json", scope
)
client = gspread.authorize(creds)

# Spreadsheet ID
SPREADSHEET_ID = "1jfruOftY0v5uBwx2NctrhczqYWLxmmJzhTU6pwlDRTQ"

# Route utama menuju sheet pertama dari spreadsheet
@app.route("/")
def home():
    sheet = client.open_by_key(SPREADSHEET_ID).sheet1
    rows = sheet.get_all_records()
    return render_template("table.html", rows=rows)

# Route untuk ambil sheet lain berdasarkan nama
@app.route("/sheet/<name>")
def sheet_data(name):
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(name)
        rows = sheet.get_all_records()
        return render_template("table.html", rows=rows)
    except Exception as e:
        return f"Sheet '{name}' tidak ditemukan. Error: {e}", 404

if __name__ == "__main__":
    app.run(debug=True)
