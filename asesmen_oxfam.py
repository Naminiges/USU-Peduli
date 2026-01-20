# asesmen_oxfam.py
# SATGAS USU Peduli - Oxfam Rapid Integrated Emergency Assessment (RIEA)
# ---------------------------------------------------------------
# - Struktur pertanyaan & pilihan jawaban di-load dari oxfam_form_spec.json (hasil konversi XLSX).
# - Jawaban disimpan dalam JSON (dict) dengan key = kolom `name` pada sheet survey (XLSForm).
# - Field teknis XLSForm (start/end/username/deviceid/phonenumber/gps/date) tidak ditampilkan
#   karena sudah digantikan meta web: posko + waktu + koordinat.
# ---------------------------------------------------------------

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import flash, redirect, render_template, request, session, url_for

# pg_data akan tersedia di project utama
#
# PENTING:
# Jangan import fungsi yang belum tentu ada (mis. pg_get_last_insert_id).
# Jika import gagal, variabel pg_get_data_lokasi bisa ikut jadi None dan
# dropdown Posko jadi kosong.
try:
    from pg_data import (
        pg_insert_asesmen_oxfam,
        pg_get_asesmen_oxfam_by_id,
        pg_get_data_lokasi,
    )

    # Optional (tidak wajib ada di semua versi pg_data.py)
    try:
        from pg_data import pg_get_last_insert_id  # type: ignore
    except Exception:
        pg_get_last_insert_id = None

except Exception:
    pg_insert_asesmen_oxfam = None
    pg_get_asesmen_oxfam_by_id = None
    pg_get_data_lokasi = None
    pg_get_last_insert_id = None


# ==========================
# CONFIG & LOADER
# ==========================

WIB = timezone(timedelta(hours=7))
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))

# Bisa override via env jika mau taruh JSON di tempat lain
_OXFAM_SPEC_PATH = os.environ.get("OXFAM_FORM_SPEC_PATH") or os.path.join(_THIS_DIR, "oxfam_form_spec.json")


def _safe_read_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _load_form_spec() -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """Load spec dari oxfam_form_spec.json.

    Format file:
    {
      "form_nodes": [...],
      "choices": {"list_name": [{"name":...,"label":...,"filter":...}, ...]}
    }
    """
    spec = _safe_read_json(_OXFAM_SPEC_PATH)
    if not isinstance(spec, dict):
        return [], {}

    nodes = spec.get("form_nodes")
    choices = spec.get("choices")

    if not isinstance(nodes, list):
        nodes = []
    if not isinstance(choices, dict):
        choices = {}

    # Normalisasi choices: pastikan list of dict
    norm_choices: Dict[str, List[Dict[str, Any]]] = {}
    for ln, arr in choices.items():
        if not isinstance(ln, str):
            continue
        if not isinstance(arr, list):
            continue
        good = []
        for it in arr:
            if isinstance(it, dict) and "name" in it:
                good.append(it)
        norm_choices[ln] = good

    return nodes, norm_choices


FORM_NODES, CHOICES = _load_form_spec()


@dataclass
class QMeta:
    name: str
    qtype: str
    label: str
    list_name: Optional[str] = None
    required: bool = False


def _walk_questions(nodes: List[Dict[str, Any]], out: Dict[str, QMeta]) -> None:
    for n in nodes or []:
        kind = n.get("kind")
        if kind == "group":
            _walk_questions(n.get("children") or [], out)
            continue
        if kind != "question":
            continue

        name = str(n.get("name") or "").strip()
        if not name:
            continue

        qtype = str(n.get("qtype") or "text").strip()
        label = str(n.get("label") or name).strip()
        list_name = n.get("list_name")
        if list_name is not None:
            list_name = str(list_name)

        required = bool(n.get("required") is True)
        out[name] = QMeta(name=name, qtype=qtype, label=label, list_name=list_name, required=required)


QUESTION_META: Dict[str, QMeta] = {}
_walk_questions(FORM_NODES, QUESTION_META)


def _get_posko_options() -> List[Dict[str, Any]]:
    """Ambil opsi POSKO dari data_lokasi.

    Posko biasanya:
    - jenis_lokasi == "Posko Pengungsian" (case-insensitive), ATAU
    - kode/id_lokasi berawalan "P-".

    Output disesuaikan dengan template asesmen_oxfam.html:
    - kode_posko
    - nama_posko
    - nama_kabkota (opsional)
    """
    if pg_get_data_lokasi is None:
        return []

    try:
        rows = pg_get_data_lokasi() or []
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue

        jenis = str(r.get("jenis_lokasi") or "").strip().lower()

        kode = (
            str(r.get("kode_lokasi") or "").strip()
            or str(r.get("id_lokasi") or "").strip()
            or str(r.get("kode") or "").strip()
            or str(r.get("kode_posko") or "").strip()
        )

        nama = (
            str(r.get("nama_lokasi") or "").strip()
            or str(r.get("nama") or "").strip()
            or str(r.get("nama_posko") or "").strip()
        )

        kab = (
            str(r.get("nama_kabkota") or "").strip()
            or str(r.get("kabupaten_kota") or "").strip()
            or str(r.get("kabkota") or "").strip()
        )

        if not kode:
            continue

        is_posko = False
        if jenis == "posko pengungsian" or ("posko" in jenis):
            is_posko = True
        if kode.upper().startswith("P-"):
            is_posko = True

        if not is_posko:
            continue

        out.append(
            {"kode_posko": kode, "nama_posko": nama or kab or kode, "nama_kabkota": kab}
        )

    out.sort(
        key=lambda x: (
            (x.get("nama_kabkota") or "").strip().lower(),
            (x.get("kode_posko") or "").strip().lower(),
            (x.get("nama_posko") or "").strip().lower(),
        )
    )
    return out


def _parse_waktu_wib(dt_local_str: str) -> datetime:
    s = (dt_local_str or "").strip()
    if not s:
        return datetime.now(WIB)

    # 1) ISO: 2026-01-20T06:56 atau 2026-01-20 06:56
    try:
        s2 = s.replace(" ", "T")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=WIB)
        return dt.astimezone(WIB)
    except Exception:
        pass

    # 2) Format UI kamu: 20/01/2026 06.56
    for fmt in ("%d/%m/%Y %H.%M", "%d/%m/%Y %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=WIB)
        except Exception:
            continue

    return datetime.now(WIB)



def _collect_answers(form: Dict[str, Any]) -> Dict[str, Any]:
    """Ambil jawaban dari request.form berdasarkan QUESTION_META."""
    jawaban: Dict[str, Any] = {}

    for name, meta in QUESTION_META.items():
        if meta.qtype == "select_multiple":
            vals = form.getlist(name)
            jawaban[name] = vals
        else:
            jawaban[name] = (form.get(name) or "").strip()

    # Meta tambahan (otomatis dari web)
    lat = (form.get("latitude") or "").strip()
    lon = (form.get("longitude") or "").strip()
    if lat and lon:
        jawaban["gps"] = {"lat": lat, "lon": lon}

    date_val = (form.get("date") or "").strip()
    if date_val:
        jawaban["date"] = date_val

    return jawaban


def _choices_name_to_label(list_name: str, value: str) -> str:
    opts = CHOICES.get(list_name) or []
    for o in opts:
        if str(o.get("name")) == str(value):
            return str(o.get("label") or value)
    return value


def _build_display_answers(jawaban_raw: Dict[str, Any]) -> Dict[str, Any]:
    """Konversi value code -> label (select_one/multiple)."""
    out: Dict[str, Any] = {}
    for key, val in (jawaban_raw or {}).items():
        meta = QUESTION_META.get(key)
        if not meta or not meta.list_name:
            out[key] = val
            continue

        if meta.qtype == "select_multiple" and isinstance(val, list):
            out[key] = [_choices_name_to_label(meta.list_name, v) for v in val]
        else:
            out[key] = _choices_name_to_label(meta.list_name, val)
    return out


# ==========================
# ROUTE REGISTRATION
# ==========================

def register_asesmen_oxfam_routes(app):

    @app.route("/asesmen_oxfam/new")
    def asesmen_oxfam_new():
        if not session.get("logged_in"):
            flash("Silakan login terlebih dahulu.")
            return redirect(url_for("login"))

        if not FORM_NODES:
            flash("Form Oxfam belum siap: file oxfam_form_spec.json belum ada / rusak.")
            return redirect(url_for("map_view"))

        posko_options = _get_posko_options()
        now_dt = datetime.now(WIB)

        # === PREFILL (WAJIB) ===
        # Template asesmen_oxfam.html menggunakan variabel `prefill.*`
        # jadi harus selalu dipass supaya tidak UndefinedError.
        lat = (request.args.get("lat") or request.args.get("latitude") or "").strip()
        lng = (request.args.get("lng") or request.args.get("lon") or request.args.get("longitude") or "").strip()
        kode_posko = (request.args.get("kode_posko") or "").strip()

        # kompatibel: bisa pakai waktu atau waktu_local di query
        waktu_q = (request.args.get("waktu_local") or request.args.get("waktu") or "").strip()
        default_waktu = now_dt.strftime("%Y-%m-%dT%H:%M")
        prefill = {
            "latitude": lat,
            "longitude": lng,
            "kode_posko": kode_posko,
            "waktu_local": waktu_q or default_waktu,  # template pakai ini
            "waktu": waktu_q or default_waktu,        # kompatibel kalau ada yg pakai ini
        }

        return render_template(
            "asesmen_oxfam.html",
            posko_options=posko_options,
            form_nodes=FORM_NODES,
            choices=CHOICES,
            today_date=now_dt.strftime("%Y-%m-%d"),
            now_ts=now_dt.strftime("%Y-%m-%dT%H:%M"),
            prefill=prefill,
            nama_relawan=session.get("nama_relawan", ""),
        )

    @app.route("/asesmen_oxfam/submit", methods=["POST"])
    def asesmen_oxfam_submit():
        if not session.get("logged_in"):
            flash("Unauthorized")
            return redirect(url_for("login"))

        if pg_insert_asesmen_oxfam is None:
            flash("Fitur Oxfam belum aktif (pg_data belum siap).")
            return redirect(url_for("map_view"))

        form = request.form

        id_relawan = session.get("id_relawan") or session.get("username") or ""
        kode_posko = (form.get("kode_posko") or "").strip() or None
        catatan = (form.get("catatan") or "").strip() or None

        # waktu: template kamu input-nya name="waktu" tapi value dari prefill.waktu_local
        # jadi di backend kita terima keduanya (waktu_local atau waktu)
        waktu_local = (form.get("waktu_local") or form.get("waktu") or "").strip()
        dt_wib = _parse_waktu_wib(waktu_local)

        # date otomatis untuk meta
        date_val = (form.get("date") or "").strip() or dt_wib.strftime("%Y-%m-%d")

        lat = (form.get("latitude") or "").strip()
        lon = (form.get("longitude") or "").strip()

        jawaban = _collect_answers(form)

        # pastikan meta terisi
        jawaban.setdefault("date", date_val)
        if lat and lon:
            jawaban.setdefault("gps", {"lat": lat, "lon": lon})

        # skor/status: sementara mengikuti pola asesmen lain (bisa dikembangkan)
        skor = 0.0
        status = "AMAN"

        ok = False
        try:
            ok = bool(
                pg_insert_asesmen_oxfam(
                    id_relawan=str(id_relawan),
                    kode_posko=kode_posko,
                    jawaban=jawaban,
                    skor=skor,
                    status=status,
                    latitude=lat,
                    longitude=lon,
                    catatan=catatan,
                    waktu=dt_wib,
                )
            )
        except Exception:
            ok = False

        if not ok:
            flash("Gagal menyimpan asesmen Oxfam.")
            return redirect(url_for("asesmen_oxfam_new"))

        # Redirect ke detail jika bisa ambil last inserted id
        asesmen_id = None
        try:
            if pg_get_last_insert_id is not None:
                asesmen_id = pg_get_last_insert_id(os.environ.get("PG_ASESMEN_OXFAM_TABLE", "public.asesmen_oxfam"))
        except Exception:
            asesmen_id = None

        flash("Asesmen Oxfam berhasil disimpan.")
        if asesmen_id:
            return redirect(url_for("asesmen_oxfam_view", asesmen_id=int(asesmen_id)))
        return redirect(url_for("map_view"))

    @app.route("/asesmen_oxfam/view/<int:asesmen_id>")
    def asesmen_oxfam_view(asesmen_id: int):
        if not session.get("logged_in"):
            flash("Silakan login terlebih dahulu.")
            return redirect(url_for("login"))

        if pg_get_asesmen_oxfam_by_id is None:
            flash("Fitur Oxfam belum aktif (pg_data belum siap).")
            return redirect(url_for("map_view"))

        row = pg_get_asesmen_oxfam_by_id(int(asesmen_id))
        if not row:
            flash("Data asesmen tidak ditemukan.")
            return redirect(url_for("map_view"))

        jawaban_raw = row.get("jawaban")
        if isinstance(jawaban_raw, str):
            try:
                jawaban_raw = json.loads(jawaban_raw)
            except Exception:
                jawaban_raw = {}
        if not isinstance(jawaban_raw, dict):
            jawaban_raw = {}

        jawaban_display = _build_display_answers(jawaban_raw)

        return render_template(
            "asesmen_oxfam_view.html",
            row=row,
            form_nodes=FORM_NODES,
            choices=CHOICES,
            jawaban_raw=jawaban_raw,
            jawaban_display=jawaban_display,
        )
