import io
import os
import csv
import re
import hmac
import time
import threading
from contextlib import closing
from datetime import datetime
from pathlib import Path
from datetime import timedelta
from urllib.parse import parse_qs, urlparse, unquote
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg2
import pyodbc
from dotenv import load_dotenv
from flask import (
    Flask,
    flash,
    g,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph,Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from psycopg2 import IntegrityError
from psycopg2.extras import RealDictCursor

BlobServiceClient = None
ContentSettings = None

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
DATA_DIR = BASE_DIR / "data"
SCHEMA_PATH = BASE_DIR / "schema.sql"


def resolve_storage_dir(env_var_name, default_relative_path):
    configured_path = os.getenv(env_var_name, "").strip()
    if configured_path:
        resolved = Path(configured_path).expanduser()
        if not resolved.is_absolute():
            resolved = BASE_DIR / resolved
    else:
        resolved = BASE_DIR / default_relative_path
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


PDF_DIR = resolve_storage_dir("TICKETS_PDF_DIR", "tickets_pdf")
REPORT_PDF_DIR = resolve_storage_dir("REPORT_PDF_DIR", "reports_pdf")


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "local-dev-secret-key")
app.permanent_session_lifetime = timedelta(hours=8)

PROCESS_TZ = os.getenv("TZ", "").strip()
if PROCESS_TZ and hasattr(time, "tzset"):
    os.environ["TZ"] = PROCESS_TZ
    try:
        time.tzset()
    except Exception:
        app.logger.warning("Could not apply process timezone '%s'.", PROCESS_TZ)

APP_TIMEZONE = os.getenv("APP_TIMEZONE", PROCESS_TZ or "UTC").strip() or "UTC"
try:
    APP_TZ = ZoneInfo(APP_TIMEZONE)
except ZoneInfoNotFoundError:
    APP_TZ = ZoneInfo("UTC")
    app.logger.warning("Invalid APP_TIMEZONE '%s'. Falling back to UTC.", APP_TIMEZONE)


def app_now():
    return datetime.now(APP_TZ)

APP_USERNAME = os.getenv("APP_USERNAME", "").strip()
APP_PASSWORD = os.getenv("APP_PASSWORD", "").strip()

_db_init_lock = threading.Lock()
_db_initialized = False

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
AZURE_STORAGE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "ticket-pdfs").strip() or "ticket-pdfs"
AZURE_TICKETS_BLOB_PREFIX = os.getenv("AZURE_TICKETS_BLOB_PREFIX", "tickets").strip().strip("/")
AZURE_REPORTS_BLOB_PREFIX = os.getenv("AZURE_REPORTS_BLOB_PREFIX", "reports").strip().strip("/")
AZURE_DOWNLOADS_BLOB_PREFIX = os.getenv("AZURE_DOWNLOADS_BLOB_PREFIX", "downloads").strip().strip("/")
AZURE_JOBS_CACHE_BLOB_NAME = os.getenv("AZURE_JOBS_CACHE_BLOB_NAME", "jobs_cache/jobs_cache.csv").strip().strip("/")


def env_flag(name, default=False):
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


AUTO_DB_BOOTSTRAP = env_flag("AUTO_DB_BOOTSTRAP", True)
PG_CONNECT_TIMEOUT = int(os.getenv("PG_CONNECT_TIMEOUT", "8").strip() or "8")
MAX_JOB_OPTIONS = int(os.getenv("MAX_JOB_OPTIONS", "2000").strip() or "2000")


def get_blob_service_client():
    global BlobServiceClient, ContentSettings

    if not AZURE_STORAGE_CONNECTION_STRING:
        return None

    if BlobServiceClient is None:
        try:
            from azure.storage.blob import BlobServiceClient as _BlobServiceClient
            from azure.storage.blob import ContentSettings as _ContentSettings

            BlobServiceClient = _BlobServiceClient
            ContentSettings = _ContentSettings
        except ImportError:
            app.logger.warning(
                "Azure Blob connection string is set, but azure-storage-blob package is not installed."
            )
            return None

    try:
        return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    except Exception as exc:
        app.logger.warning("Could not initialize Azure Blob client: %s", exc)
        return None


def upload_pdf_to_blob(blob_name, pdf_bytes):
    blob_service = get_blob_service_client()
    if blob_service is None:
        return None

    container_client = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)
    try:
        container_client.create_container()
    except Exception:
        pass

    blob_client = container_client.get_blob_client(blob_name)
    content_settings = ContentSettings(content_type="application/pdf") if ContentSettings else None
    blob_client.upload_blob(pdf_bytes, overwrite=True, content_settings=content_settings)
    return blob_client.url


def upload_download_audit_blob(category, filename, file_bytes, mimetype):
    blob_service = get_blob_service_client()
    if blob_service is None:
        return None

    safe_category = re.sub(r"[^a-zA-Z0-9_\-]", "_", str(category or "download"))
    safe_name = re.sub(r"[^a-zA-Z0-9._\-]", "_", str(filename or "file"))
    stamp = app_now().strftime("%Y/%m/%d/%H%M%S_%f")
    blob_name = (
        f"{AZURE_DOWNLOADS_BLOB_PREFIX}/{safe_category}/{stamp}_{safe_name}"
        if AZURE_DOWNLOADS_BLOB_PREFIX
        else f"{safe_category}/{stamp}_{safe_name}"
    )

    remote_ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "")[:120]
    endpoint = str(request.endpoint or "")[:120]

    metadata = {
        "endpoint": re.sub(r"[^a-zA-Z0-9_\-]", "_", endpoint),
        "remote_ip": re.sub(r"[^a-zA-Z0-9_\-:., ]", "_", remote_ip),
        "downloaded_at": datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
    }

    container_client = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)
    try:
        container_client.create_container()
    except Exception:
        pass

    blob_client = container_client.get_blob_client(blob_name)
    content_settings = ContentSettings(content_type=mimetype) if ContentSettings and mimetype else None
    blob_client.upload_blob(file_bytes, overwrite=True, content_settings=content_settings, metadata=metadata)
    return blob_client.url


def upload_jobs_cache_blob(file_bytes):
    blob_service = get_blob_service_client()
    if blob_service is None or not AZURE_JOBS_CACHE_BLOB_NAME:
        return None

    container_client = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)
    try:
        container_client.create_container()
    except Exception:
        pass

    blob_client = container_client.get_blob_client(AZURE_JOBS_CACHE_BLOB_NAME)
    content_settings = ContentSettings(content_type="text/csv") if ContentSettings else None
    blob_client.upload_blob(file_bytes, overwrite=True, content_settings=content_settings)
    return blob_client.url


def download_jobs_cache_blob():
    blob_service = get_blob_service_client()
    if blob_service is None or not AZURE_JOBS_CACHE_BLOB_NAME:
        return None

    try:
        blob_client = blob_service.get_blob_client(
            container=AZURE_STORAGE_CONTAINER,
            blob=AZURE_JOBS_CACHE_BLOB_NAME,
        )
        content = blob_client.download_blob().readall()
        return content if content else None
    except Exception as exc:
        app.logger.warning("Could not download jobs cache blob '%s': %s", AZURE_JOBS_CACHE_BLOB_NAME, exc)
        return None


def delete_pdf_blob_if_needed(pdf_path):
    if not pdf_path or not str(pdf_path).lower().startswith("http"):
        return False

    blob_service = get_blob_service_client()
    if blob_service is None:
        return False

    parsed = urlparse(str(pdf_path))
    marker = f"/{AZURE_STORAGE_CONTAINER}/"
    idx = parsed.path.find(marker)
    if idx < 0:
        return False

    blob_name = unquote(parsed.path[idx + len(marker):]).lstrip("/")
    if not blob_name:
        return False

    try:
        blob_service.get_blob_client(container=AZURE_STORAGE_CONTAINER, blob=blob_name).delete_blob(delete_snapshots="include")
        return True
    except Exception as exc:
        app.logger.warning("Could not delete blob '%s': %s", blob_name, exc)
        return False


def write_temp_pdf_for_print(pdf_bytes, name_prefix):
    cache_dir = BASE_DIR / "tmp_print"
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp = app_now().strftime("%Y%m%d_%H%M%S_%f")
    path = cache_dir / f"{name_prefix}_{stamp}.pdf"
    with open(path, "wb") as f:
        f.write(pdf_bytes)
    return str(path)


def format_ticket_datetime(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return ""
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return text
    return dt.strftime("%m-%d-%Y - %H:%M")


@app.template_filter("ticket_datetime")
def ticket_datetime_filter(value):
    return format_ticket_datetime(value)


def resolve_jobs_csv_path():
    csv_url = os.getenv("JOBS_CSV_URL", "").strip()
    if csv_url:
        cache_path_raw = os.getenv("JOBS_CSV_CACHE_PATH", "").strip()
        if cache_path_raw:
            cache_path = Path(cache_path_raw)
            if not cache_path.is_absolute():
                cache_path = BASE_DIR / cache_path
        else:
            cache_path = DATA_DIR / "jobs_remote_cache.csv"

        cache_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            download_jobs_csv_from_url(csv_url, cache_path)
            app.logger.info("Jobs CSV downloaded from URL to %s", cache_path)

            try:
                with open(cache_path, "rb") as f:
                    blob_url = upload_jobs_cache_blob(f.read())
                if blob_url:
                    app.logger.info("Jobs CSV uploaded to blob cache at %s", blob_url)
            except Exception as exc:
                app.logger.warning("Jobs CSV upload to blob cache failed: %s", exc)

            return cache_path
        except Exception as exc:
            blob_content = download_jobs_cache_blob()
            if blob_content:
                tmp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
                with open(tmp_path, "wb") as f:
                    f.write(blob_content)
                tmp_path.replace(cache_path)
                app.logger.warning(
                    "Jobs CSV URL download failed (%s). Using blob cache '%s'.",
                    exc,
                    AZURE_JOBS_CACHE_BLOB_NAME,
                )
                return cache_path

            if cache_path.exists():
                app.logger.warning(
                    "Jobs CSV URL download failed (%s). Using cached file at %s.",
                    exc,
                    cache_path,
                )
                return cache_path
            app.logger.warning("Jobs CSV URL download failed and no cache is available: %s", exc)

    configured = os.getenv("JOBS_CSV_PATH", "").strip()
    candidates = []

    if configured:
        configured_path = Path(configured)
        if not configured_path.is_absolute():
            configured_path = BASE_DIR / configured_path
        candidates.append(configured_path)
    else:
        candidates.append(BASE_DIR / "data" / "jobs.csv")
        candidates.append(Path(r"G:\My Drive\Jobs Master ALL.csv"))

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def normalize_jobs_csv_url(url):
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    if "drive.google.com" not in host:
        return url

    file_id = extract_google_drive_file_id(url)
    if file_id:
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url


def extract_google_drive_file_id(url):
    parsed = urlparse(url)
    path = parsed.path or ""

    file_match = re.search(r"/file/d/([^/]+)", path)
    if file_match:
        return file_match.group(1)

    query_id = parse_qs(parsed.query).get("id", [])
    if query_id:
        return query_id[0]

    return None


def download_jobs_csv_from_url(url, destination_path):
    download_url = normalize_jobs_csv_url(url)
    request = Request(
        download_url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; McCrakenTicketSystem/1.0)"
        },
    )

    with urlopen(request, timeout=30) as response:
        content = response.read()

    if not content:
        raise RuntimeError("Downloaded CSV is empty.")

    head = content[:512].lstrip().lower()
    if head.startswith(b"<!doctype html") or head.startswith(b"<html"):
        raise RuntimeError(
            "URL returned HTML instead of CSV. Ensure the Google Drive file is shared and downloadable."
        )

    tmp_path = destination_path.with_suffix(destination_path.suffix + ".tmp")
    with open(tmp_path, "wb") as f:
        f.write(content)
    tmp_path.replace(destination_path)


def create_db_connection():
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        conn = psycopg2.connect(
            database_url,
            cursor_factory=RealDictCursor,
            connect_timeout=PG_CONNECT_TIMEOUT,
        )
    else:
        host = os.getenv("PGHOST", "").strip()
        database = os.getenv("PGDATABASE", "").strip()
        user = os.getenv("PGUSER", "").strip()
        password = os.getenv("PGPASSWORD", "").strip()
        missing = []
        if not host:
            missing.append("PGHOST")
        if not database:
            missing.append("PGDATABASE")
        if not user:
            missing.append("PGUSER")
        if not password:
            missing.append("PGPASSWORD")
        if missing:
            raise RuntimeError(
                "Missing PostgreSQL settings: "
                + ", ".join(missing)
                + ". Set DATABASE_URL or populate .env with PGHOST/PGDATABASE/PGUSER/PGPASSWORD."
            )
        conn = psycopg2.connect(
            host=host,
            port=int(os.getenv("PGPORT", "5432").strip()),
            dbname=database,
            user=user,
            password=password,
            sslmode=os.getenv("PGSSLMODE", "require").strip(),
            cursor_factory=RealDictCursor,
            connect_timeout=PG_CONNECT_TIMEOUT,
        )
    conn.autocommit = False
    return conn


def get_db():
    ensure_app_initialized()
    if "db" not in g:
        g.db = create_db_connection()
    return g.db


@app.teardown_appcontext
def close_db(_exception):
    db = g.pop("db", None)
    if db is not None:
        db.close() 


def init_db():
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    with closing(create_db_connection()) as conn:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema_sql = f.read()
        with conn.cursor() as cursor:
            cursor.execute(schema_sql)
        ensure_db_migrations(conn)
        conn.commit()


def ensure_app_initialized():
    global _db_initialized
    if _db_initialized:
        return

    # In managed environments, schema/bootstrap can be run separately to keep worker startup fast.
    if not AUTO_DB_BOOTSTRAP:
        _db_initialized = True
        return

    with _db_init_lock:
        if _db_initialized:
            return
        init_db()
        _db_initialized = True


def ensure_db_migrations(conn):
    with conn.cursor() as cursor:
        cursor.execute("ALTER TABLE jobs_cache ADD COLUMN IF NOT EXISTS tax_exempt TEXT NOT NULL DEFAULT ''")
        cursor.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS customer_snapshot TEXT NOT NULL DEFAULT ''")
        cursor.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS tax_exempt TEXT NOT NULL DEFAULT ''")
        cursor.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS cost REAL NOT NULL DEFAULT 0")
        cursor.execute("ALTER TABLE tickets ALTER COLUMN pdf_path DROP NOT NULL")
        cursor.execute("ALTER TABLE tickets ALTER COLUMN pdf_blob DROP NOT NULL")
        cursor.execute("ALTER TABLE trucks ADD COLUMN IF NOT EXISTS truck_size TEXT NOT NULL DEFAULT ''")
        cursor.execute("ALTER TABLE trucks ADD COLUMN IF NOT EXISTS hauled_by TEXT NOT NULL DEFAULT ''")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_jobs (
                id BIGSERIAL PRIMARY KEY,
                job_code TEXT NOT NULL,
                job_name TEXT NOT NULL,
                customer TEXT NOT NULL DEFAULT '',
                tax_exempt TEXT NOT NULL DEFAULT '',
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                UNIQUE(job_code, job_name)
            )
            """
        )


def refresh_jobs_on_startup():
    auto_refresh = os.getenv("AUTO_REFRESH_JOBS_ON_STARTUP", "1").strip().lower()
    if auto_refresh not in {"1", "true", "yes", "on"}:
        app.logger.info("Startup jobs refresh disabled by AUTO_REFRESH_JOBS_ON_STARTUP.")
        return

    with app.app_context():
        db = get_db()
        try:
            count = refresh_jobs_cache(db)
            db.commit()
            app.logger.info("Startup jobs refresh complete. %s rows synced.", count)
        except Exception as exc:
            db.rollback()
            app.logger.warning("Startup jobs refresh skipped/failed: %s", exc)


def next_ticket_number(db):
    year = app_now().year
    with db.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ticket_sequence (ticket_year, last_value)
            VALUES (%s, 0)
            ON CONFLICT (ticket_year) DO NOTHING
            """,
            (year,),
        )
        cursor.execute(
            """
            UPDATE ticket_sequence
            SET last_value = last_value + 1
            WHERE ticket_year = %s
            RETURNING last_value
            """,
            (year,),
        )
        row = cursor.fetchone()
    next_value = int(row["last_value"])
    ticket_number = f"DT-{year}-{next_value:06d}"
    return ticket_number, year, next_value


def truck_size_to_axle_index(truck_size):
    size_text = str(truck_size or "").strip().lower()
    if not size_text:
        return None

    match = re.search(r"axle\s*([0-9]+(?:\.[0-9]+)?)", size_text)
    if not match:
        return None

    axle_value = float(match.group(1))
    axle_index = int(axle_value)
    if axle_value != axle_index:
        return None
    return axle_index if 1 <= axle_index <= 9 else None


def calculate_ticket_cost(truck, material, quantity_num):
    # print(f"Calculating cost for truck: {truck}, material: {material}, quantity: {quantity_num}")
    if not truck or not material:
        return 0.0
    track_size_map = { 
        "Axle 1":"axle_1", "Tandem":"tandem","TriAxle":"triaxle","Axle 4_5":"axle_4_5","Axle 6":"axle_6","Semi":"semi","HydVac":"hydvac","Hydrovac":"hydvac","DIRT_IN":"dirt_in"
    }
    price_per_load = float(material.get(track_size_map.get(truck.get("truck_size"))) or 0)
    return round(price_per_load * quantity_num, 2)




def to_pdf_bytes(ticket):
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    company_header_raw = os.getenv("COMPANY_HEADER", "").strip()
    if company_header_raw:
        company_header_lines = [line.strip() for line in company_header_raw.split("|") if line.strip()]
    else:
        company_header_lines = [
            "McCracken Materials, LLC",
            "13675 McCracken Road",
            "Garfield Heights, Ohio 44125",
            "Phone: (216) 206-2600",
        ]

    def draw_field(label, value, x, y, w, h, label_width=70, font_size=8):
        pdf.rect(x, y - h, w, h)
        pdf.setFont("Helvetica-Bold", font_size)
        pdf.drawString(x + 4, y - 12, str(label))
        pdf.line(x + label_width, y - h, x + label_width, y)
        pdf.setFont("Helvetica", font_size)
        pdf.drawString(x + label_width + 4, y - 12, str(value or ""))

    def draw_ticket_section(copy_title, include_signature_line, section_top, section_bottom):
        left = 36
        right = width - 36
        box_w = right - left

        section_h = section_top - section_bottom

        pdf.setLineWidth(1)
        pdf.rect(left, section_bottom + 10, box_w, section_h - 20)

        company_start_y = section_top - 22

        for index, line in enumerate(company_header_lines):
            if index == 0:
                pdf.setFont("Helvetica-Bold", 9)
            else:
                pdf.setFont("Helvetica", 7)

            pdf.drawCentredString(width / 2, company_start_y - (index * 10), line)

        y_top = section_top - 72
        ticket_type ="DUMP TICKET" if ticket["direction"] == "IN" else "MATERIAL TICKET"

        pdf.setFont("Helvetica-Bold", 13)
        pdf.drawCentredString(width / 2, y_top, ticket_type)

        pdf.setFont("Helvetica-Bold", 8)
        pdf.drawCentredString(width / 2, y_top - 14, copy_title.upper())

        draw_field(
            "Ticket #",
            ticket["ticket_number"],
            left + 10,
            y_top - 26,
            250,
            18,
            label_width=55,
            font_size=8
        )

        draw_field(
            "Date/Time",
            format_ticket_datetime(ticket["created_at"]),
            left + 270,
            y_top - 26,
            box_w - 280,
            18,
            label_width=62,
            font_size=8
        )

        direction_text = "IN  [X]   OUT [ ]" if ticket["direction"] == "IN" else "IN  [ ]   OUT [X]"

        draw_field(
            "Direction",
            direction_text,
            left + 10,
            y_top - 48,
            box_w - 20,
            18,
            label_width=62,
            font_size=8
        )

        draw_field(
            "Job #",
            ticket["job_code_snapshot"],
            left + 10,
            y_top - 70,
            180,
            18,
            label_width=42,
            font_size=8
        )

        draw_field(
            "Job Name",
            ticket["job_name_snapshot"],
            left + 195,
            y_top - 70,
            box_w - 205,
            18,
            label_width=58,
            font_size=8
        )

        draw_field(
            "Customer",
            ticket.get("customer_snapshot", ""),
            left + 10,
            y_top - 92,
            box_w - 20,
            18,
            label_width=58,
            font_size=8
        )

        draw_field(
            "Truck #",
            ticket["truck_number_snapshot"],
            left + 10,
            y_top - 114,
            180,
            18,
            label_width=50,
            font_size=8
        )

        draw_field(
            "Material",
            ticket["material_name_snapshot"],
            left + 195,
            y_top - 114,
            box_w - 205,
            18,
            label_width=52,
            font_size=8
        )

        draw_field(
            "Quantity",
            f"{ticket['quantity']}",
            left + 10,
            y_top - 136,
            180,
            18,
            label_width=55,
            font_size=8
        )

        draw_field(
            "Unit",
            ticket["unit"],
            left + 195,
            y_top - 136,
            120,
            18,
            label_width=32,
            font_size=8
        )

        # draw_field(
        #     "Cost",
        #     f"${float(ticket.get('cost', 0) or 0):.2f}",
        #     left + 320,
        #     y_top - 136,
        #     box_w - 330,
        #     18,
        #     label_width=34,
        #     font_size=8
        # )

        notes_y = y_top - 158
        notes_h = 42

        pdf.rect(left + 10, notes_y - notes_h, box_w - 20, notes_h)

        pdf.setFont("Helvetica-Bold", 8)
        pdf.drawString(left + 16, notes_y - 12, "Notes")

        pdf.setFont("Helvetica", 8)
        note_text = ticket.get("notes", "") or ""

        if len(note_text) > 120:
            note_text = note_text[:117] + "..."

        pdf.drawString(left + 58, notes_y - 12, note_text)

        if include_signature_line:
            sig_y = notes_y - notes_h - 12

            draw_field(
                "Driver Name",
                "",
                left + 10,
                sig_y,
                (box_w - 30) / 2,
                18,
                label_width=62,
                font_size=8
            )

            draw_field(
                "Signature",
                "",
                left + 20 + (box_w - 30) / 2,
                sig_y,
                (box_w - 30) / 2,
                18,
                label_width=58,
                font_size=8
            )

        pdf.setFont("Helvetica", 7)
        pdf.drawRightString(
            right - 8,
            section_bottom + 18,
            f"Printed: {format_ticket_datetime(app_now())}"
        )

    middle_y = height / 2

    draw_ticket_section(
        "Driver Copy Signature Required",
        True,
        section_top=height,
        section_bottom=middle_y
    )

    draw_ticket_section(
        "Internal Billing Copy",
        False,
        section_top=middle_y,
        section_bottom=0
    )

    pdf.setDash(3, 3)
    pdf.line(36, middle_y, width - 36, middle_y)
    pdf.setDash()

    pdf.showPage()
    pdf.save()

    buffer.seek(0)
    return buffer.read()

def report_to_pdf_bytes(tickets, totals_by_unit, totals_by_material, filters):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)

    styles = getSampleStyleSheet()
    normal = styles["Normal"]
    bold = styles["Heading3"]

    elements = []

    # ---- Title ----
    elements.append(Paragraph("Ticket Report", bold))
    elements.append(
        Paragraph(
            f"Generated: {app_now().strftime('%m-%d-%Y %H:%M')}",
            normal,
        )
    )
    elements.append(Spacer(1, 12))

    # ---- Tickets Table ----
    table_data = [
        ["Ticket #", "Date/Time", "Customer", "Dir", "Material", "Qty", "Unit", "Cost"]
    ]

    for t in tickets:
        table_data.append([
            Paragraph(str(t["ticket_number"]), normal),
            Paragraph(format_ticket_datetime(t["created_at"]), normal),
            Paragraph(t["customer_snapshot"] or "", normal),
            Paragraph(t["direction"], normal),
            Paragraph(t["material_name_snapshot"], normal),
            Paragraph(f"{t['quantity']:.2f}", normal),
            Paragraph(t["unit"], normal),
            Paragraph(f"${float(t.get('cost', 0) or 0):.2f}", normal),
        ])

    ticket_table = Table(
        table_data,
        colWidths=[65, 85, 110, 30, 90, 40, 35, 55],
        repeatRows=1
    )

    ticket_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ("ALIGN", (5, 1), (5, -1), "RIGHT"),
        ("ALIGN", (7, 1), (7, -1), "RIGHT"),
        ("ALIGN", (3, 1), (3, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ]))

    elements.append(ticket_table)
    elements.append(Spacer(1, 20))

    # ---- Totals By Unit ----
    elements.append(Paragraph("Totals By Unit", bold))
    totals_unit_table = Table(
        [["Unit", "Total Quantity", "Total Cost"]] +
        [[r["unit"], f"{r['total_quantity']:.2f}", f"${float(r.get('total_cost', 0) or 0):.2f}"] for r in totals_by_unit],
        colWidths=[90, 110, 110],
    )
    totals_unit_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(totals_unit_table)
    elements.append(Spacer(1, 20))

    # ---- Totals By Material ----
    elements.append(Paragraph("Totals By Material", bold))
    totals_mat_table = Table(
        [["Material", "Unit", "Total Quantity", "Total Cost"]] +
        [
            [
                r["material_name_snapshot"],
                r["unit"],
                f"{r['total_quantity']:.2f}",
                f"${float(r.get('total_cost', 0) or 0):.2f}",
            ]
            for r in totals_by_material
        ],
        colWidths=[180, 70, 100, 100],
    )
    totals_mat_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(totals_mat_table)

    doc.build(elements)
    buffer.seek(0)
    return buffer.read()


def materials_report_to_pdf_bytes(materials):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)

    styles = getSampleStyleSheet()
    normal = styles["Normal"]
    bold = styles["Heading3"]

    elements = []
    elements.append(Paragraph("Materials Export", bold))
    elements.append(
        Paragraph(
            f"Generated: {app_now().strftime('%m-%d-%Y %H:%M')}",
            normal,
        )
    )
    elements.append(Spacer(1, 12))

    table_data = [[
        "Material", "Direction", "Price per Cubic Yard",
        "Axle 1", "Tandem", "TriAxle", "Axle 4-5", "Axle 6", "Semi", "HydVac", "Dirt In",
        "Status",
    ]]

    for m in materials:
        table_data.append([
            Paragraph(str(m["material_name"] or ""), normal),
            Paragraph(str(m["direction"] or ""), normal),
            Paragraph(f"${float(m['cost_per_cy']):.2f}" if m["cost_per_cy"] is not None else "", normal),
            Paragraph(f"{float(m['axle_1']):.2f}" if m["axle_1"] is not None else "", normal),
            Paragraph(f"{float(m['tandem']):.2f}" if m["tandem"] is not None else "", normal),
            Paragraph(f"{float(m['triaxle']):.2f}" if m["triaxle"] is not None else "", normal),
            Paragraph(f"{float(m['axle_4_5']):.2f}" if m["axle_4_5"] is not None else "", normal),
            Paragraph(f"{float(m['axle_6']):.2f}" if m["axle_6"] is not None else "", normal),
            Paragraph(f"{float(m['semi']):.2f}" if m["semi"] is not None else "", normal),
            Paragraph(f"{float(m['hydvac']):.2f}" if m["hydvac"] is not None else "", normal),
            Paragraph(f"{float(m['dirt_in']):.2f}" if m["dirt_in"] is not None else "", normal),
            Paragraph("Active" if m["active"] else "Inactive", normal),
        ])

    table = Table(
        table_data,
        colWidths=[90, 45, 50, 38, 38, 38, 38, 38, 38, 45, 38, 38, 45],
        repeatRows=1,
    )
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ("ALIGN", (2, 1), (11, -1), "RIGHT"),
        ("ALIGN", (1, 1), (1, -1), "CENTER"),
        ("ALIGN", (12, 1), (12, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ]))

    elements.append(table)
    doc.build(elements)
    buffer.seek(0)
    return buffer.read()

def save_pdf(ticket_number, pdf_bytes):
    year = app_now().year
    if AZURE_STORAGE_CONNECTION_STRING:
        blob_name = f"{AZURE_TICKETS_BLOB_PREFIX}/{year}/{ticket_number}.pdf" if AZURE_TICKETS_BLOB_PREFIX else f"{year}/{ticket_number}.pdf"
        try:
            blob_url = upload_pdf_to_blob(blob_name, pdf_bytes)
            if blob_url:
                return blob_url
        except Exception as exc:
            app.logger.warning("Azure Blob upload failed for ticket %s: %s. Falling back to local storage.", ticket_number, exc)

    year_dir = PDF_DIR / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = year_dir / f"{ticket_number}.pdf"
    with open(pdf_path, "wb") as f:
        f.write(pdf_bytes)
    return str(pdf_path)


def print_pdf_file(pdf_path):
    if os.name != "nt":
        raise RuntimeError("Automatic printing is currently implemented for Windows only.")
    os.startfile(pdf_path, "print")


def is_active_column_boolean(db, table_name):
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = 'active'
            """,
            (table_name,),
        )
        row = cursor.fetchone()
    return bool(row) and row["data_type"] == "boolean"


def validate_material_admin_password(admin_password):
    configured_password = os.getenv("MATERIAL_ADMIN_PASSWORD", "").strip()
    if not configured_password:
        return False, "Material admin password is not configured on server."
    if not hmac.compare_digest(admin_password, configured_password):
        return False, "Password failed."
    return True, ""


def parse_material_active_value(raw_value, use_boolean):
    value = str(raw_value or "").strip().upper()
    if value in {"1", "TRUE", "T", "YES", "Y", "ACTIVE", "A"}:
        return True if use_boolean else 1
    if value in {"0", "FALSE", "F", "NO", "N", "INACTIVE", "I"}:
        return False if use_boolean else 0
    raise ValueError("active must be 1/0, TRUE/FALSE, YES/NO, ACTIVE/INACTIVE")


def parse_materials_upload_rows(uploaded_file, filename):
    ext = Path(filename or "").suffix.lower()

    if ext == ".csv":
        text_stream = io.TextIOWrapper(uploaded_file.stream, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(text_stream)
        rows = []
        for idx, row in enumerate(reader, start=2):
            rows.append((idx, row))
        return rows

    if ext == ".xlsx":
        try:
            from openpyxl import load_workbook
        except ImportError as exc:
            raise RuntimeError("Excel upload requires the openpyxl package.") from exc

        workbook = load_workbook(uploaded_file, data_only=True)
        sheet = workbook.active
        header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True), None)
        if not header_row:
            return []

        headers = [str(col).strip() if col is not None else "" for col in header_row]
        rows = []
        for row_number, values in enumerate(sheet.iter_rows(min_row=2, values_only=True), start=2):
            row_dict = {}
            for header, value in zip(headers, values):
                row_dict[header] = "" if value is None else str(value)
            rows.append((row_number, row_dict))
        return rows

    raise RuntimeError("Unsupported file type. Please upload a .csv or .xlsx file.")


def upsert_job_cache_row(db, job_code, job_name, customer, tax_exempt, active, source_updated_at, refreshed_at):
    with db.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO jobs_cache (job_code, job_name, customer, tax_exempt, active, source_updated_at, refreshed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(job_code) DO UPDATE SET
                job_name = excluded.job_name,
                customer = excluded.customer,
                tax_exempt = excluded.tax_exempt,
                active = excluded.active,
                source_updated_at = excluded.source_updated_at,
                refreshed_at = excluded.refreshed_at
            """,
            (job_code, job_name, customer, tax_exempt, active, source_updated_at, refreshed_at),
        )


def refresh_jobs_cache(db):
    app.logger.info("Refreshing jobs cache...")
    csv_file = resolve_jobs_csv_path()
    if csv_file is not None:

        now = app_now().isoformat(timespec="seconds")
        synced = 0

        with open(csv_file, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise RuntimeError("CSV is missing a header row.")

            def first_present(*names):
                for name in names:
                    if name in reader.fieldnames:
                        return name
                return None

            job_code_col = first_present("job_code", "Job #", "Job#", "Job Number")
            job_name_col = first_present("job_name", "Job Name")
            customer_col = first_present("customer", "Customer Name")
            tax_exempt_col = first_present("tax_exempt", "Tax Exempt", "TaxExempt")
            active_col = first_present("active", "Job Status")
            source_updated_at_col = first_present("source_updated_at")

            missing = []
            if not job_code_col:
                missing.append("job_code or Job #")
            if not job_name_col:
                missing.append("job_name or Job Name")
            if missing:
                raise RuntimeError(f"CSV missing required columns: {', '.join(missing)}")

            def parse_active_value(raw_value):
                value = str(raw_value or "").strip().upper()
                if not value:
                    return 1
                if value in {"1", "A", "ACTIVE", "Y", "YES", "TRUE", "T"}:
                    return 1
                if value in {"0", "I", "C", "INACTIVE", "N", "NO", "FALSE", "F"}:
                    return 0
                try:
                    return 1 if int(value) == 1 else 0
                except ValueError:
                    return 1

            for row in reader:
                job_code = str(row.get(job_code_col) or "").strip()
                if not job_code:
                    continue

                job_name = str(row.get(job_name_col) or "").strip()
                customer = str(row.get(customer_col) or "").strip() if customer_col else ""
                tax_exempt = str(row.get(tax_exempt_col) or "").strip() if tax_exempt_col else ""
                active_raw = str(row.get(active_col) or "").strip() if active_col else ""
                active = parse_active_value(active_raw)
                source_updated_at = (
                    str(row.get(source_updated_at_col) or "").strip() if source_updated_at_col else ""
                ) or None

                upsert_job_cache_row(
                    db=db,
                    job_code=job_code,
                    job_name=job_name,
                    customer=customer,
                    tax_exempt=tax_exempt,
                    active=active,
                    source_updated_at=source_updated_at,
                    refreshed_at=now,
                )
                synced += 1
        app.logger.info("Jobs cache refreshed from CSV. %s rows synced.", synced)

        return synced

    odbc_conn = os.getenv("REMOTE_SQL_ODBC_CONNECTION_STRING")
    if not odbc_conn:
        raise RuntimeError(
            "No jobs source found. Set JOBS_CSV_PATH, place CSV at data/jobs.csv, "
            "or configure REMOTE_SQL_ODBC_CONNECTION_STRING."
        )

    query = os.getenv(
        "JOBS_SQL_QUERY",
        """
        SELECT
            CAST(job_code AS NVARCHAR(100)) AS job_code,
            CAST(job_name AS NVARCHAR(255)) AS job_name,
            CAST(customer AS NVARCHAR(255)) AS customer,
            CAST(active AS INT) AS active,
            source_updated_at
        FROM jobs
        """,
    )

    with pyodbc.connect(odbc_conn, timeout=10) as conn:
        cursor = conn.cursor()
        rows = cursor.execute(query).fetchall()

    now = app_now().isoformat(timespec="seconds")
    for row in rows:
        job_code = str(row.job_code).strip()
        job_name = str(row.job_name).strip() if row.job_name is not None else ""
        customer = str(row.customer).strip() if row.customer is not None else ""
        active = int(row.active) if row.active is not None else 1
        source_updated_at = (
            str(row.source_updated_at) if getattr(row, "source_updated_at", None) is not None else None
        )

        upsert_job_cache_row(
            db=db,
            job_code=job_code,
            job_name=job_name,
            customer=customer,
            tax_exempt="",
            active=active,
            source_updated_at=source_updated_at,
            refreshed_at=now,
        )
    return len(rows)

def list_jobs(db):
    with db.cursor() as cursor:
        cursor.execute(
        """
        SELECT id, job_code, job_name, customer
        FROM jobs_cache
        WHERE active = 1
        ORDER BY job_code
        """
        )
        return cursor.fetchall()


def list_ticket_jobs(db):
    with db.cursor() as cursor:
        cursor.execute(
        """
        SELECT
            ('cache:' || id::text) AS job_key,
            job_code,
            job_name,
            customer,
            tax_exempt
        FROM jobs_cache
        WHERE active = 1

        UNION ALL

        SELECT
            ('manual:' || id::text) AS job_key,
            job_code,
            job_name,
            customer,
            tax_exempt
        FROM manual_jobs
        WHERE active = 1

        ORDER BY job_code, job_name
        LIMIT %s
        """,
        (MAX_JOB_OPTIONS,),
        )
        return cursor.fetchall()


def list_recent_tickets(db, limit=5):
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                id,
                ticket_number,
                created_at,
                direction,
                customer_snapshot,
                truck_number_snapshot,
                material_name_snapshot,
                quantity,
                unit
            FROM tickets
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return cursor.fetchall()


def split_job_entry(job_entry):
    if " - " in job_entry:
        job_code, job_name = [part.strip() for part in job_entry.split(" - ", 1)]
    else:
        job_code = job_entry.strip()
        job_name = job_entry.strip()
    return job_code, job_name


def get_or_create_manual_job(db, job_entry):
    job_code, job_name = split_job_entry(job_entry)
    now = app_now().isoformat(timespec="seconds")
    with db.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO manual_jobs (job_code, job_name, customer, tax_exempt, active, created_at)
            VALUES (%s, %s, %s, %s, 1, %s)
            ON CONFLICT (job_code, job_name)
            DO UPDATE SET active = 1
            RETURNING id, job_code, job_name, customer, tax_exempt
            """,
            (job_code, job_name, "", "New", now),
        )
        return cursor.fetchone()


def get_selected_job(db, selected_job_id):
    if not selected_job_id:
        return None, None

    if selected_job_id.startswith("cache:"):
        job_id = selected_job_id.split(":", 1)[1]
        if not job_id.isdigit():
            return None, None
        with db.cursor() as cursor:
            cursor.execute(
                "SELECT id, job_code, job_name, customer, tax_exempt FROM jobs_cache WHERE id = %s",
                (job_id,),
            )
            return cursor.fetchone(), "cache"

    if selected_job_id.startswith("manual:"):
        job_id = selected_job_id.split(":", 1)[1]
        if not job_id.isdigit():
            return None, None
        with db.cursor() as cursor:
            cursor.execute(
                "SELECT id, job_code, job_name, customer, tax_exempt FROM manual_jobs WHERE id = %s",
                (job_id,),
            )
            return cursor.fetchone(), "manual"

    if selected_job_id.isdigit():
        with db.cursor() as cursor:
            cursor.execute(
                "SELECT id, job_code, job_name, customer, tax_exempt FROM jobs_cache WHERE id = %s",
                (selected_job_id,),
            )
            return cursor.fetchone(), "cache"

    return None, None

# def list_trucks(db):
#     return db.execute(
#         "SELECT id, truck_number, description, truck_size, hauled_by, active FROM trucks WHERE active = 1 ORDER BY truck_number"
#     ).fetchall()
def list_customers(db):
    with db.cursor() as cursor:
        cursor.execute(
        "SELECT id, customer_name FROM customers ORDER BY customer_name"
        )
        return cursor.fetchall()


def get_customer_by_name(db, customer_name):
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, customer_name
            FROM customers
            WHERE LOWER(TRIM(customer_name)) = LOWER(TRIM(%s))
            LIMIT 1
            """,
            (customer_name,),
        )
        return cursor.fetchone()

def list_trucks(db):
    with db.cursor() as cursor:
        cursor.execute(
        "SELECT id, truck_number, notes AS description, truck_size, trucking_company AS hauled_by, active FROM trucks_main WHERE active = TRUE ORDER BY truck_number"
        )
        return cursor.fetchall()

def list_materials(db, direction=None):
    with db.cursor() as cursor:
        if direction:
            cursor.execute(
                "SELECT id, material AS material_name, active, direction FROM material_price WHERE active = TRUE AND direction = %s ORDER BY material_name",
                (direction,),
            )
        else:
            cursor.execute(
                "SELECT id, material AS material_name, active, direction FROM material_price WHERE active = TRUE ORDER BY material_name"
            )
        return cursor.fetchall()

@app.before_request
def require_login():
    open_endpoints = {"login", "logout", "static", "healthz"}
    if request.endpoint in open_endpoints:
        return
    if not session.get("logged_in"):
        return redirect(url_for("login", next=request.url))


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("logged_in"):
        return redirect(url_for("new_ticket"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        if (
            APP_USERNAME
            and APP_PASSWORD
            and hmac.compare_digest(username, APP_USERNAME)
            and hmac.compare_digest(password, APP_PASSWORD)
        ):
            session["logged_in"] = True
            session.permanent = True
            next_url = request.args.get("next") or url_for("new_ticket")
            parsed_next = urlparse(next_url)
            if parsed_next.netloc:
                next_url = url_for("new_ticket")
            return redirect(next_url)
        flash("Invalid username or password.", "error")
    return render_template("login.html")


@app.post("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def home():
    return redirect(url_for("new_ticket"))


@app.get("/healthz")
def healthz():
    return "ok", 200


@app.route("/tickets/new", methods=["GET", "POST"])
def new_ticket():
    db = get_db()
    created_ticket_id = None

    if request.method == "POST":
        direction = request.form.get("direction", "IN").strip().upper()
        job_id = request.form.get("job_id", "").strip()
        job_entry = request.form.get("job_entry", "").strip()
        truck_id = request.form.get("truck_id", "").strip()
        truck_entry = request.form.get("truck_entry", "").strip()
        material_id = request.form.get("material_id", "").strip()
        material_entry = request.form.get("material_entry", "").strip()
        customer_id = request.form.get("customer_id", "").strip()
        quantity = (request.form.get("quantity") or "1").strip()
        unit = (request.form.get("unit") or "Load").strip()
        notes = request.form.get("notes", "").strip()
        auto_print = request.form.get("auto_print") == "on"
        use_now = request.form.get("use_now") == "on"
        custom_datetime = request.form.get("custom_datetime")

        if direction not in {"IN", "OUT"}:
            flash("Direction must be IN or OUT.", "error")
            return redirect(url_for("new_ticket"))
        if not all([job_entry, truck_entry, material_entry, quantity, unit]):
            flash("Job, truck, material, quantity, and unit are required.", "error")
            return redirect(url_for("new_ticket"))
        if not all([job_id, truck_id, material_id, customer_id]):
            flash("Please select job, customer, truck, and material from dropdown lists.", "error")
            return redirect(url_for("new_ticket"))
        # print(f"Received new ticket data: direction={direction}, job_id={job_id}, job_entry={job_entry}, truck_id={truck_id}, truck_entry={truck_entry}, material_id={material_id}, material_entry={material_entry}, customer_id={customer_id}, quantity={quantity}, unit={unit}, notes={notes}, auto_print={auto_print}, use_now={use_now}, custom_datetime={custom_datetime}")

        job = None
        truck = None
        material = None
        customer = None
        customer_snapshot = ""
        tax_exempt_snapshot = ""
        selected_job_source = None
        if job_id:
            job, selected_job_source = get_selected_job(db, job_id)
        if truck_id:
            with db.cursor() as cursor:
                cursor.execute("SELECT id, truck_number, truck_size FROM trucks_main WHERE id = %s", (truck_id,))
                truck = cursor.fetchone()
        if material_id:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id, material AS material_name,
                           axle_1, tandem, triaxle, axle_4_5, axle_6, semi, hydvac, dirt_in
                    FROM material_price
                    WHERE id = %s
                    """,
                    (material_id,),
                )
                material = cursor.fetchone()
        if customer_id:
            with db.cursor() as cursor:
                cursor.execute("SELECT id, customer_name FROM customers WHERE id = %s", (customer_id,))
                customer = cursor.fetchone()
            if customer:
                customer_snapshot = (customer["customer_name"] or "").strip()

        if job:
            job_id_value = job["id"] if selected_job_source == "cache" else None
            job_code_snapshot = job["job_code"]
            job_name_snapshot = job["job_name"]
            tax_exempt_snapshot = (job["tax_exempt"] or "").strip()
            if not customer_snapshot:
                customer_snapshot = (job["customer"] or "").strip()
        else:
            manual_job = get_or_create_manual_job(db, job_entry)
            job_id_value = None
            job_code_snapshot = manual_job["job_code"]
            job_name_snapshot = manual_job["job_name"]
            tax_exempt_snapshot = (manual_job["tax_exempt"] or "").strip() or "New"
            if not customer_snapshot:
                customer_snapshot = (manual_job["customer"] or "").strip()

        if truck:
            truck_id_value = truck["id"]
            truck_number_snapshot = truck["truck_number"]
        else:
            truck_id_value = None
            truck_number_snapshot = truck_entry

        if material:
            material_id_value = material["id"]
            material_name_snapshot = material["material_name"]
        else:
            material_id_value = None
            material_name_snapshot = material_entry

        try:
            quantity_num = float(quantity)
        except ValueError:
            flash("Quantity must be numeric.", "error")
            return redirect(url_for("new_ticket"))

        ticket_cost = calculate_ticket_cost(truck, material, quantity_num)

        try:
            ticket_number, ticket_year, seq = next_ticket_number(db)
            if use_now:
                created_at = app_now().isoformat(timespec="seconds")
            else:
                if not custom_datetime:
                    flash("Please select a valid date and time.", "error")
                    return redirect(url_for("new_ticket"))
                try:
                    created_at = datetime.fromisoformat(custom_datetime).isoformat(timespec="seconds")
                except ValueError:
                    flash("Invalid date format.", "error")
                    return redirect(url_for("new_ticket"))

            row = {
                "ticket_number": ticket_number,
                "created_at": created_at,
                "direction": direction,
                "job_code_snapshot": job_code_snapshot,
                "job_name_snapshot": job_name_snapshot,
                "tax_exempt": tax_exempt_snapshot,
                "customer_snapshot": customer_snapshot,
                "truck_number_snapshot": truck_number_snapshot,
                "material_name_snapshot": material_name_snapshot,
                "quantity": quantity_num,
                "unit": unit,
                "cost": ticket_cost,
                "notes": notes,
            }
            pdf_bytes = to_pdf_bytes(row)
            pdf_path = save_pdf(ticket_number, pdf_bytes)
            app.logger.info("Generated PDF for ticket %s.", ticket_number)

            with db.cursor() as cursor:
                cursor.execute(
                """
                INSERT INTO tickets (
                    ticket_number, ticket_year, ticket_sequence, direction, created_at,
                    job_id, job_code_snapshot, job_name_snapshot, tax_exempt, customer_snapshot, truck_id, truck_number_snapshot,
                    material_id, material_name_snapshot, quantity, unit, cost, notes, pdf_path, pdf_blob
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    ticket_number,
                    ticket_year,
                    seq,
                    direction,
                    created_at,
                    job_id_value,
                    job_code_snapshot,
                    job_name_snapshot,
                    tax_exempt_snapshot,
                    customer_snapshot,
                    truck_id_value,
                    truck_number_snapshot,
                    material_id_value,
                    material_name_snapshot,
                    quantity_num,
                    unit,
                    ticket_cost,
                    notes,
                    pdf_path,
                    pdf_bytes,
                ),
                )
                inserted_ticket = cursor.fetchone()
                if inserted_ticket:
                    created_ticket_id = inserted_ticket["id"]
            db.commit()
        except Exception:
            db.rollback()
            raise

        if auto_print:
            if created_ticket_id is not None:
                # Browser handles printing on the user's device and default printer.
                return redirect(url_for("ticket_pdf", ticket_id=created_ticket_id, inline=1))
            flash("Ticket saved, but PDF preview could not be opened automatically.", "error")
            return redirect(url_for("new_ticket"))

        flash(f"Ticket {ticket_number} created.", "success")
        return redirect(url_for("new_ticket"))
        # return redirect(url_for("search_tickets", ticket_number=ticket_number))

    else:
        direction = request.args.get("direction", "IN").strip().upper()

    return render_template(
        "ticket_new.html",
        jobs=list_ticket_jobs(db),
        customers =list_customers(db),
        trucks=list_trucks(db),
        materials=list_materials(db,direction=direction),
        recent_tickets=list_recent_tickets(db, limit=5),
    )

@app.get("/materials")
def get_materials():
    db = get_db()
    direction = request.args.get("direction")

    materials = list_materials(db, direction)

    return {
        "materials": [
            {"id": m["id"], "name": m["material_name"]}
            for m in materials
        ]
    }

@app.post("/jobs/refresh")
def refresh_jobs():
    db = get_db()
    try:
        count = refresh_jobs_cache(db)
        db.commit()
        flash(f"Jobs refresh complete. {count} rows synced.", "success")
    except Exception as exc:
        db.rollback()
        flash(f"Jobs refresh failed: {exc}", "error")
    return redirect(url_for("new_ticket"))


@app.route("/tickets/search", methods=["GET"])
def search_tickets():
    db = get_db()
    ticket_number = request.args.get("ticket_number", "").strip()
    truck = request.args.get("truck", "").strip()
    job = request.args.get("job", "").strip()
    material = request.args.get("material", "").strip()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()

    query = """
        SELECT
            id,
            ticket_number,
            created_at,
            direction,
            job_code_snapshot,
            tax_exempt,
            customer_snapshot,
            truck_number_snapshot,
            material_name_snapshot,
            cost,
            CASE
                WHEN pdf_blob IS NOT NULL OR COALESCE(pdf_path, '') <> '' THEN TRUE
                ELSE FALSE
            END AS has_pdf
        FROM tickets
        WHERE 1 = 1
    """
    params = []
    if ticket_number:
        query += " AND ticket_number ILIKE %s"
        params.append(f"%{ticket_number}%")
    if truck:
        query += " AND truck_number_snapshot ILIKE %s"
        params.append(f"%{truck}%")
    if job:
        query += " AND job_code_snapshot ILIKE %s"
        params.append(f"%{job}%")
    if material:
        query += " AND material_name_snapshot ILIKE %s"
        params.append(f"%{material}%")
    if date_from:
        query += " AND date(created_at) >= date(%s)"
        params.append(date_from)
    if date_to:
        query += " AND date(created_at) <= date(%s)"
        params.append(date_to)
    query += " ORDER BY id DESC LIMIT 200"

    with db.cursor() as cursor:
        cursor.execute(query, tuple(params))
        tickets = cursor.fetchall()
    return render_template("ticket_search.html", tickets=tickets)


@app.route("/reports", methods=["GET"])
def reports():
    db = get_db()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()
    direction = request.args.get("direction", "").strip().upper()
    job_id = request.args.get("job_id", "").strip()
    material_id = request.args.get("material_id", "").strip()
    offset = int(request.args.get("offset", 0))
    if not date_from and not date_to:
        date_to = app_now().date().isoformat()
        date_from = (app_now().date() - timedelta(days=14)).isoformat()

    where = ["1 = 1"]
    params = []

    if date_from:
        where.append("date(t.created_at) >= date(%s)")
        params.append(date_from)
    if date_to:
        where.append("date(t.created_at) <= date(%s)")
        params.append(date_to)
    if direction in {"IN", "OUT"}:
        where.append("t.direction = %s")
        params.append(direction)
    if job_id:
        where.append("t.job_id = %s")
        params.append(job_id)
    if material_id:
        where.append("t.material_id = %s")
        params.append(material_id)

    where_sql = " AND ".join(where)
    app.logger.debug("Report query WHERE clause: %s with params %s", where_sql, params)

    with db.cursor() as cursor:
        cursor.execute(
        f"""
        SELECT
            t.id,
            t.ticket_number,
            t.created_at,
            t.direction,
            t.job_code_snapshot,
            t.job_name_snapshot,
            t.customer_snapshot,
            t.tax_exempt,
            t.material_name_snapshot,
            t.truck_number_snapshot,
            t.quantity,
            t.unit,
            t.cost
        FROM tickets t
        WHERE {where_sql}
        ORDER BY 
            t.customer_snapshot ASC,
            CASE 
                WHEN t.direction = 'IN' THEN 1
                WHEN t.direction = 'OUT' THEN 2
                ELSE 3 
            END,
            t.id DESC
        LIMIT 20 OFFSET %s
        """,
        tuple(params+[offset]),
        )
        tickets = cursor.fetchall()

    with db.cursor() as cursor:
        cursor.execute(
        f"""
         SELECT t.unit,
             COALESCE(SUM(t.quantity), 0) AS total_quantity,
             COALESCE(SUM(t.cost), 0) AS total_cost
        FROM tickets t
        WHERE {where_sql}
        GROUP BY t.unit
        ORDER BY t.unit
        """,
        tuple(params),
        )
        totals_by_unit = cursor.fetchall()

    with db.cursor() as cursor:
        cursor.execute(
        f"""
         SELECT t.material_name_snapshot,
             t.unit,
             COALESCE(SUM(t.quantity), 0) AS total_quantity,
             COALESCE(SUM(t.cost), 0) AS total_cost
        FROM tickets t
        WHERE {where_sql}
        GROUP BY t.material_name_snapshot, t.unit
        ORDER BY t.material_name_snapshot, t.unit
        """,
        tuple(params),
        )
        totals_by_material = cursor.fetchall()

    with db.cursor() as cursor:
        cursor.execute(
        f"""
         SELECT t.direction,
             t.unit,
             COALESCE(SUM(t.quantity), 0) AS total_quantity,
             COALESCE(SUM(t.cost), 0) AS total_cost
        FROM tickets t
        WHERE {where_sql}
        GROUP BY t.direction, t.unit
        ORDER BY t.direction, t.unit
        """,
        tuple(params),
        )
        totals_by_direction = cursor.fetchall()

    return render_template(
        "reports.html",
        tickets=tickets,
        offset=offset,
        totals_by_unit=totals_by_unit,
        totals_by_material=totals_by_material,
        totals_by_direction=totals_by_direction,
        jobs=list_jobs(db),
        materials=list_materials(db,direction=direction),
        filters={
            "date_from": date_from,
            "date_to": date_to,
            "direction": direction,
            "job_id": job_id,
            "material_id": material_id,
        },
    )


@app.route("/reports/export.csv", methods=["GET"])
def export_reports_csv():
    db = get_db()
    date_from = request.args.get("date_from", "").strip()
    date_to = request.args.get("date_to", "").strip()
    direction = request.args.get("direction", "").strip().upper()
    job_id = request.args.get("job_id", "").strip()
    material_id = request.args.get("material_id", "").strip()

    where = ["1 = 1"]
    params = []

    if date_from:
        where.append("date(t.created_at) >= date(%s)")
        params.append(date_from)
    if date_to:
        where.append("date(t.created_at) <= date(%s)")
        params.append(date_to)
    if direction in {"IN", "OUT"}:
        where.append("t.direction = %s")
        params.append(direction)
    if job_id:
        where.append("t.job_id = %s")
        params.append(job_id)
    if material_id:
        where.append("t.material_id = %s")
        params.append(material_id)

    where_sql = " AND ".join(where)

    with db.cursor() as cursor:
        cursor.execute(
        f"""
        SELECT
            t.ticket_number,
            t.created_at,
            t.direction,
            t.job_code_snapshot,
            t.job_name_snapshot,
            t.customer_snapshot,
            t.truck_number_snapshot,
            t.material_name_snapshot,
            t.quantity,
            t.unit,
            t.cost
        FROM tickets t
        WHERE {where_sql}
        ORDER BY t.id DESC
        LIMIT 1000
        """,
        tuple(params),
        )
        tickets = cursor.fetchall()

    with db.cursor() as cursor:
        cursor.execute(
        f"""
         SELECT t.unit,
             COALESCE(SUM(t.quantity), 0) AS total_quantity,
             COALESCE(SUM(t.cost), 0) AS total_cost
        FROM tickets t
        WHERE {where_sql}
        GROUP BY t.unit
        ORDER BY t.unit
        """,
        tuple(params),
        )
        totals_by_unit = cursor.fetchall()

    with db.cursor() as cursor:
        cursor.execute(
        f"""
         SELECT t.material_name_snapshot,
             t.unit,
             COALESCE(SUM(t.quantity), 0) AS total_quantity,
             COALESCE(SUM(t.cost), 0) AS total_cost
        FROM tickets t
        WHERE {where_sql}
        GROUP BY t.material_name_snapshot, t.unit
        ORDER BY t.material_name_snapshot, t.unit
        """,
        tuple(params),
        )
        totals_by_material = cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(
        [
            "Ticket Number",
            "Created At",
            "Direction",
            "Job Code",
            "Job Name",
            "Customer",
            "Truck",
            "Material",
            "Quantity",
            "Unit",
            "Cost",
        ]
    )
    for t in tickets:
        writer.writerow(
            [
                t["ticket_number"],
                format_ticket_datetime(t["created_at"]),
                t["direction"],
                t["job_code_snapshot"],
                t["job_name_snapshot"],
                t["customer_snapshot"],
                t["truck_number_snapshot"],
                t["material_name_snapshot"],
                f"{t['quantity']:.2f}",
                t["unit"],
                f"{float(t.get('cost', 0) or 0):.2f}",
            ]
        )

    writer.writerow([])
    writer.writerow(["Totals by Unit"])
    writer.writerow(["Unit", "Total Quantity", "Total Cost"])
    for total in totals_by_unit:
        writer.writerow([total["unit"], f"{total['total_quantity']:.2f}", f"{float(total.get('total_cost', 0) or 0):.2f}"])

    writer.writerow([])
    writer.writerow(["Totals by Material"])
    writer.writerow(["Material", "Unit", "Total Quantity", "Total Cost"])
    for total in totals_by_material:
        writer.writerow(
            [
                total["material_name_snapshot"],
                total["unit"],
                f"{total['total_quantity']:.2f}",
                f"{float(total.get('total_cost', 0) or 0):.2f}",
            ]
        )

    csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
    output.close()
    csv_bytes.seek(0)

    stamp = app_now().strftime("%Y%m%d_%H%M%S")
    download_filename = f"ticket_report_{stamp}.csv"
    csv_payload = csv_bytes.getvalue()
    try:
        upload_download_audit_blob(
            category="reports_csv",
            filename=download_filename,
            file_bytes=csv_payload,
            mimetype="text/csv",
        )
    except Exception as exc:
        app.logger.warning("Could not audit-upload report CSV download: %s", exc)

    return send_file(
        io.BytesIO(csv_payload),
        mimetype="text/csv",
        as_attachment=True,
        download_name=download_filename,
    )
@app.get("/reports/print")
def print_reports():
    db = get_db()

    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")
    direction = request.args.get("direction", "")
    job_id = request.args.get("job_id", "")
    material_id = request.args.get("material_id", "")

    where = ["1=1"]
    params = []

    if date_from:
        where.append("date(t.created_at)>=date(%s)")
        params.append(date_from)

    if date_to:
        where.append("date(t.created_at)<=date(%s)")
        params.append(date_to)

    if direction in {"IN", "OUT"}:
        where.append("t.direction=%s")
        params.append(direction)

    if job_id:
        where.append("t.job_id=%s")
        params.append(job_id)

    if material_id:
        where.append("t.material_id=%s")
        params.append(material_id)

    where_sql = " AND ".join(where)

    with db.cursor() as cursor:
        cursor.execute(
        f"""
        SELECT *
        FROM tickets t
        WHERE {where_sql}
        ORDER BY
            t.customer_snapshot ASC,
            CASE
                WHEN t.direction='IN' THEN 1
                WHEN t.direction='OUT' THEN 2
            END,
            t.id DESC
        """,
        tuple(params),
        )
        tickets = cursor.fetchall()

    with db.cursor() as cursor:
        cursor.execute(
        f"""
         SELECT unit,
             SUM(quantity) AS total_quantity,
             SUM(cost) AS total_cost
        FROM tickets t
        WHERE {where_sql}
        GROUP BY unit
        """,
        tuple(params),
        )
        totals_by_unit = cursor.fetchall()

    with db.cursor() as cursor:
        cursor.execute(
        f"""
         SELECT material_name_snapshot,
             unit,
             SUM(quantity) AS total_quantity,
             SUM(cost) AS total_cost
        FROM tickets t
        WHERE {where_sql}
        GROUP BY material_name_snapshot, unit
        """,
        tuple(params),
        )
        totals_by_material = cursor.fetchall()

    pdf_bytes = report_to_pdf_bytes(
        tickets,
        totals_by_unit,
        totals_by_material,
        {
            "date_from": date_from,
            "date_to": date_to,
            "direction": direction,
            "job_id": job_id,
            "material_id": material_id,
        },
    )

    stamp = app_now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"ticket_report_{stamp}.pdf"

    if AZURE_STORAGE_CONNECTION_STRING:
        blob_name = f"{AZURE_REPORTS_BLOB_PREFIX}/{report_filename}" if AZURE_REPORTS_BLOB_PREFIX else report_filename
        try:
            upload_pdf_to_blob(blob_name, pdf_bytes)
        except Exception as exc:
            app.logger.warning("Azure Blob upload failed for report %s: %s", report_filename, exc)

        if os.name == "nt":
            try:
                temp_print_path = write_temp_pdf_for_print(pdf_bytes, "report")
                print_pdf_file(temp_print_path)
            except Exception:
                pass

        try:
            upload_download_audit_blob(
                category="reports_pdf",
                filename=report_filename,
                file_bytes=pdf_bytes,
                mimetype="application/pdf",
            )
        except Exception as exc:
            app.logger.warning("Could not audit-upload report PDF download: %s", exc)

        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=report_filename,
        )

    pdf_path = REPORT_PDF_DIR / report_filename
    with open(pdf_path, "wb") as f:
        f.write(pdf_bytes)

    try:
        print_pdf_file(str(pdf_path))
    except Exception:
        pass

    try:
        upload_download_audit_blob(
            category="reports_pdf",
            filename=pdf_path.name,
            file_bytes=pdf_bytes,
            mimetype="application/pdf",
        )
    except Exception as exc:
        app.logger.warning("Could not audit-upload local report PDF download: %s", exc)

    return send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=pdf_path.name,
    )

@app.get("/tickets/<int:ticket_id>/pdf")
def ticket_pdf(ticket_id):
    db = get_db()
    inline_pdf = str(request.args.get("inline", "")).strip().lower() in {"1", "true", "yes", "on"}
    with db.cursor() as cursor:
        cursor.execute("SELECT ticket_number, pdf_path, pdf_blob FROM tickets WHERE id = %s", (ticket_id,))
        row = cursor.fetchone()
    if not row:
        flash("Ticket not found.", "error")
        return redirect(url_for("search_tickets"))

    ticket_filename = f"{row['ticket_number']}.pdf"
    pdf_blob = row.get("pdf_blob")
    if pdf_blob is not None:
        try:
            upload_download_audit_blob(
                category="ticket_pdf",
                filename=ticket_filename,
                file_bytes=pdf_blob,
                mimetype="application/pdf",
            )
        except Exception as exc:
            app.logger.warning("Could not audit-upload ticket PDF download: %s", exc)

        return send_file(
            io.BytesIO(pdf_blob),
            mimetype="application/pdf",
            as_attachment=not inline_pdf,
            download_name=ticket_filename,
        )

    pdf_path = str(row.get("pdf_path") or "").strip()
    if pdf_path and not pdf_path.lower().startswith("http"):
        path_obj = Path(pdf_path)
        if path_obj.exists():
            return send_file(
                path_obj,
                mimetype="application/pdf",
                as_attachment=not inline_pdf,
                download_name=ticket_filename,
            )

    flash("No PDF exists for this ticket yet. Use Generate PDF.", "error")
    return redirect(url_for("search_tickets", ticket_number=row["ticket_number"]))


@app.post("/tickets/<int:ticket_id>/print")
def print_ticket(ticket_id):
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute("SELECT ticket_number, pdf_path, pdf_blob FROM tickets WHERE id = %s", (ticket_id,))
        row = cursor.fetchone()
    if not row:
        flash("Ticket not found.", "error")
        return redirect(url_for("search_tickets"))

    try:
        pdf_path = (row.get("pdf_path") or "").strip()
        pdf_blob = row.get("pdf_blob")
        if pdf_path and not pdf_path.lower().startswith("http") and Path(pdf_path).exists():
            print_pdf_file(pdf_path)
        elif pdf_blob is not None:
            temp_print_path = write_temp_pdf_for_print(pdf_blob, row["ticket_number"])
            print_pdf_file(temp_print_path)
        else:
            flash("No PDF exists for this ticket yet. Use Generate PDF first.", "error")
            return redirect(url_for("search_tickets", ticket_number=row["ticket_number"]))
        flash(f"Print sent for {row['ticket_number']}.", "success")
    except Exception as exc:
        flash(f"Print failed: {exc}", "error")
    return redirect(url_for("search_tickets", ticket_number=row["ticket_number"]))


@app.post("/tickets/<int:ticket_id>/generate-pdf")
def generate_ticket_pdf(ticket_id):
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                id,
                ticket_number,
                created_at,
                direction,
                job_code_snapshot,
                job_name_snapshot,
                tax_exempt,
                customer_snapshot,
                truck_number_snapshot,
                material_name_snapshot,
                quantity,
                unit,
                cost,
                notes,
                pdf_path,
                pdf_blob
            FROM tickets
            WHERE id = %s
            """,
            (ticket_id,),
        )
        row = cursor.fetchone()

    if not row:
        flash("Ticket not found.", "error")
        return redirect(url_for("search_tickets"))

    try:
        pdf_bytes = to_pdf_bytes(row)
        pdf_path = save_pdf(row["ticket_number"], pdf_bytes)
        with db.cursor() as cursor:
            cursor.execute(
                "UPDATE tickets SET pdf_path = %s, pdf_blob = %s WHERE id = %s",
                (pdf_path, pdf_bytes, ticket_id),
            )
        db.commit()
        flash(f"PDF generated for {row['ticket_number']}.", "success")
    except Exception as exc:
        db.rollback()
        flash(f"Could not generate PDF: {exc}", "error")

    return redirect(url_for("search_tickets", ticket_number=row["ticket_number"]))


@app.post("/tickets/<int:ticket_id>/void")
def void_ticket(ticket_id):
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute("SELECT ticket_number, pdf_path FROM tickets WHERE id = %s", (ticket_id,))
        row = cursor.fetchone()

    if not row:
        flash("Ticket not found.", "error")
        return redirect(request.referrer or url_for("search_tickets"))

    try:
        with db.cursor() as cursor:
            cursor.execute("DELETE FROM tickets WHERE id = %s", (ticket_id,))
        db.commit()

        pdf_path = (row.get("pdf_path") or "").strip()
        if pdf_path:
            try:
                if pdf_path.lower().startswith("http"):
                    delete_pdf_blob_if_needed(pdf_path)
                else:
                    path_obj = Path(pdf_path)
                    if path_obj.exists():
                        path_obj.unlink()
            except OSError:
                # Ticket is removed from DB even if the old PDF file cleanup fails.
                pass

        flash(f"Ticket {row['ticket_number']} was voided.", "success")
    except Exception as exc:
        db.rollback()
        flash(f"Could not void ticket: {exc}", "error")

    return redirect(request.referrer or url_for("search_tickets"))


@app.route("/admin/trucks", methods=["GET", "POST"])
def admin_trucks():
    db = get_db()
    if request.method == "POST":
        truck_number = request.form.get("truck_number", "").strip()
        description = request.form.get("description", "").strip()
        truck_size = request.form.get("truck_size", "").strip()
        hauled_by = request.form.get("hauled_by", "").strip()
        license_plate = request.form.get("license_plate", "").strip()
        if not truck_number:
            flash("Truck number is required.", "error")
            return redirect(url_for("admin_trucks"))
        try:
            active_value = True if is_active_column_boolean(db, "trucks_main") else 1
            with db.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO trucks_main (truck_number, notes, truck_size, trucking_company, license_plate, active) VALUES (%s, %s, %s, %s, %s, %s)",
                    (truck_number, description, truck_size, hauled_by, license_plate, active_value),
                )
            db.commit()
            flash("Truck added.", "success")
        except IntegrityError:
            db.rollback()
            flash("Truck number already exists.", "error")
        return redirect(url_for("admin_trucks"))

    truck_query = request.args.get("q", "").strip()

    with db.cursor() as cursor:
        if truck_query:
            like_query = f"%{truck_query}%"
            cursor.execute(
                """
                SELECT id, truck_number, notes AS description, truck_size, trucking_company AS hauled_by, license_plate, active
                FROM trucks_main
                WHERE truck_number ILIKE %s
                   OR COALESCE(truck_size, '') ILIKE %s
                   OR COALESCE(trucking_company, '') ILIKE %s
                   OR COALESCE(license_plate, '') ILIKE %s
                ORDER BY truck_number
                """,
                (like_query, like_query, like_query, like_query),
            )
        else:
            cursor.execute(
                "SELECT id, truck_number, notes AS description, truck_size, trucking_company AS hauled_by, license_plate, active FROM trucks_main ORDER BY truck_number"
            )
        rows = cursor.fetchall()
    return render_template("admin_trucks.html", trucks=rows, truck_query=truck_query)


@app.post("/admin/trucks/<int:truck_id>/toggle")
def toggle_truck(truck_id):
    db = get_db()
    with db.cursor() as cursor:
        if is_active_column_boolean(db, "trucks_main"):
            cursor.execute(
                "UPDATE trucks_main SET active = NOT COALESCE(active, FALSE) WHERE id = %s",
                (truck_id,),
            )
        else:
            cursor.execute(
                "UPDATE trucks_main SET active = CASE WHEN COALESCE(active, 0) = 1 THEN 0 ELSE 1 END WHERE id = %s",
                (truck_id,),
            )
    db.commit() 
    return redirect(url_for("admin_trucks"))


@app.get("/admin/materials")
def admin_materials():
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, cat, material AS material_name, cost_per_cy, direction,
            axle_1, tandem, triaxle, axle_4_5, axle_6, semi, hydvac, dirt_in, active
            FROM material_price
            ORDER BY active DESC, direction, material
            """
        )
        rows = cursor.fetchall()
    return render_template("admin_materials.html", materials=rows)


@app.get("/admin/materials/export.pdf")
def export_materials_pdf():
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, cat, material AS material_name, cost_per_cy, direction,
            axle_1, tandem, triaxle, axle_4_5, axle_6, semi, hydvac, dirt_in, active
            FROM material_price
            ORDER BY active DESC, direction, material
            """
        )
        rows = cursor.fetchall()

    pdf_bytes = materials_report_to_pdf_bytes(rows)
    stamp = app_now().strftime("%Y%m%d_%H%M%S")
    filename = f"materials_export_{stamp}.pdf"

    try:
        upload_download_audit_blob(
            category="materials_pdf",
            filename=filename,
            file_bytes=pdf_bytes,
            mimetype="application/pdf",
        )
    except Exception as exc:
        app.logger.warning("Could not audit-upload materials PDF download: %s", exc)

    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=filename,
    )


@app.get("/admin/materials/export.csv")
def export_materials_csv():
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, cat, material AS material_name, cost_per_cy, direction,
            axle_1, tandem, triaxle, axle_4_5, axle_6, semi, hydvac, dirt_in, active
            FROM material_price
            ORDER BY id
            """
        )
        rows = cursor.fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id",
        "cat",
        "material_name",
        "cost_per_cy",
        "direction",
        "axle_1",
        "tandem",
        "triaxle",
        "axle_4_5",
        "axle_6",
        "semi",
        "hydvac",
        "dirt_in",
        "active",
    ])

    for row in rows:
        writer.writerow([
            row["id"],
            row["cat"],
            row["material_name"],
            row["cost_per_cy"],
            row["direction"],
            row["axle_1"],
            row["tandem"],
            row["triaxle"],
            row["axle_4_5"],
            row["axle_6"],
            row["semi"],
            row["hydvac"],
            row["dirt_in"],
            1 if row["active"] else 0,
        ])

    csv_bytes = output.getvalue().encode("utf-8")
    stamp = app_now().strftime("%Y%m%d_%H%M%S")
    filename = f"materials_export_{stamp}.csv"

    try:
        upload_download_audit_blob(
            category="materials_csv",
            filename=filename,
            file_bytes=csv_bytes,
            mimetype="text/csv",
        )
    except Exception as exc:
        app.logger.warning("Could not audit-upload materials CSV download: %s", exc)

    return send_file(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.post("/admin/materials/import")
def import_materials_csv():
    db = get_db()
    admin_password = request.form.get("admin_password", "")
    password_ok, password_message = validate_material_admin_password(admin_password)
    if not password_ok:
        flash(f"{password_message} CSV was not imported.", "error")
        return redirect(url_for("admin_materials"))

    uploaded = request.files.get("materials_file")
    if uploaded is None or not (uploaded.filename or "").strip():
        flash("Please choose a CSV or Excel file to upload.", "error")
        return redirect(url_for("admin_materials"))

    try:
        raw_rows = parse_materials_upload_rows(uploaded, uploaded.filename)
    except Exception as exc:
        flash(f"Could not read upload: {exc}", "error")
        return redirect(url_for("admin_materials"))

    if not raw_rows:
        flash("Uploaded file contains no data rows.", "error")
        return redirect(url_for("admin_materials"))

    required_columns = {
        "id",
        "cat",
        "material_name",
        "cost_per_cy",
        "direction",
        "axle_1",
        "tandem",
        "triaxle",
        "axle_4_5",
        "axle_6",
        "semi",
        "hydvac",
        "dirt_in",
        "active",
    }
    axle_keys = [
    "axle_1",
    "tandem",
    "triaxle",
    "axle_4_5",
    "axle_6",
    "semi",
    "hydvac",
    "dirt_in",
    ]

    sample_columns = {str(k).strip().lower() for k in raw_rows[0][1].keys()}
    missing = sorted(required_columns - sample_columns)
    if missing:
        flash("Missing required columns: " + ", ".join(missing), "error")
        return redirect(url_for("admin_materials"))

    use_boolean_active = is_active_column_boolean(db, "material_price")
    parsed_rows = []

    for row_number, row in raw_rows:
        row_map = {str(k).strip().lower(): row.get(k) for k in row.keys()}
        try:
            raw_id = str(row_map.get("id") or "").strip()
            material_id = int(raw_id) if raw_id else None
            cat = int(str(row_map.get("cat") or "").strip())
            material_name = str(row_map.get("material_name") or "").strip()
            cost_per_cy = float(str(row_map.get("cost_per_cy") or "").strip())
            direction = str(row_map.get("direction") or "").strip().upper()
            if not material_name:
                raise ValueError("material_name is required")
            if direction not in {"IN", "OUT"}:
                raise ValueError("direction must be IN or OUT")

            axle_values = []
            for key in axle_keys:
                val = str(row_map.get(key) or "").strip()
                axle_values.append(None if val == "" else float(val))

            active_value = parse_material_active_value(row_map.get("active"), use_boolean_active)

            parsed_rows.append((
                material_id,
                cat,
                material_name,
                direction,
                cost_per_cy,
                *axle_values,
                active_value,
            ))
        except ValueError as exc:
            flash(f"Row {row_number}: {exc}", "error")
            return redirect(url_for("admin_materials"))

    try:
        updated_count = 0
        inserted_count = 0
        with db.cursor() as cursor:
            for parsed in parsed_rows:
                material_id = parsed[0]
                values = parsed[1:]

                if material_id is not None:
                    cursor.execute(
                        """
                        UPDATE material_price
                        SET cat = %s,
                            material = %s,
                            direction = %s,
                            cost_per_cy = %s,
                            axle_1 = %s,
                            tandem = %s,
                            triaxle = %s,
                            axle_4_5 = %s,
                            axle_6 = %s,
                            semi = %s,
                            hydvac = %s,
                            dirt_in = %s,
                            active = %s
                        WHERE id = %s
                        """,
                        (*values, material_id),
                    )

                    if cursor.rowcount > 0:
                        updated_count += cursor.rowcount
                        continue

                cursor.execute(
                    """
                    INSERT INTO material_price (
                        cat, material, cost_per_cy, direction,
                        axle_1, tandem, triaxle, axle_4_5, axle_6, semi, hydvac, dirt_in,
                        active
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    values,
                )
                inserted_count += 1

        db.commit()
        flash(
            f"Materials import complete. Updated {updated_count} row(s), inserted {inserted_count} row(s).",
            "success",
        )
    except Exception as exc:
        db.rollback()
        flash(f"Could not import materials: {exc}", "error")

    return redirect(url_for("admin_materials"))


@app.post("/admin/materials/<int:material_id>/edit")
def edit_material(material_id):
    flash("Direct material editing is disabled. Use CSV upload to update materials.", "error")
    return redirect(url_for("admin_materials"))


@app.route("/admin/customers", methods=["GET", "POST"])
def admin_customers():
    db = get_db()
    if request.method == "POST":
        customer_name = request.form.get("customer_name", "").strip()
        full_address = request.form.get("full_address", "").strip()
        contact_person = request.form.get("contact_person", "").strip()
        phone_number = request.form.get("phone_number", "").strip()
        notes = request.form.get("notes", "").strip()

        if not customer_name:
            flash("Customer name is required.", "error")
            return redirect(url_for("admin_customers"))

        existing_customer = get_customer_by_name(db, customer_name)
        if existing_customer:
            flash("Customer already exists.", "error")
            return redirect(url_for("admin_customers"))

        try:
            with db.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO customers (customer_name, full_address, contact_person, phone_number, notes)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (customer_name, full_address, contact_person, phone_number, notes),
                )
            db.commit()
            flash("Customer added.", "success")
        except Exception as exc:
            db.rollback()
            flash(f"Could not add customer: {exc}", "error")

        return redirect(url_for("admin_customers"))

    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, customer_name, full_address, contact_person, phone_number, notes
            FROM customers
            ORDER BY customer_name
            """
        )
        rows = cursor.fetchall()
    return render_template("admin_customers.html", customers=rows)


@app.post("/admin/materials/<int:material_id>/toggle")
def toggle_material(material_id):
    flash("Direct material status changes are disabled. Use CSV upload to update materials.", "error")
    return redirect(url_for("admin_materials"))


if __name__ == "__main__":
    ensure_app_initialized()
    refresh_jobs_on_startup()
    host = os.getenv("FLASK_RUN_HOST", "127.0.0.1")
    port = int(os.getenv("PORT", os.getenv("FLASK_RUN_PORT", "5000")))
    debug = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}
    app.run(debug=debug, host=host, port=port)
    