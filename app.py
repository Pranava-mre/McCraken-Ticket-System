import io
import os
import csv
import re
from contextlib import closing
from datetime import datetime
from pathlib import Path
from datetime import timedelta
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

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
    url_for,
)
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph,Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from psycopg2 import IntegrityError
from psycopg2.extras import RealDictCursor

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
DATA_DIR = BASE_DIR / "data"
PDF_DIR = BASE_DIR / "tickets_pdf"
SCHEMA_PATH = BASE_DIR / "schema.sql"
REPORT_PDF_DIR = BASE_DIR / "reports_pdf"
REPORT_PDF_DIR.mkdir(parents=True, exist_ok=True)


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "local-dev-secret-key")


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
            return cache_path
        except Exception as exc:
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
        conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
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
        )
    conn.autocommit = False
    return conn


def get_db():
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


def ensure_db_migrations(conn):
    with conn.cursor() as cursor:
        cursor.execute("ALTER TABLE jobs_cache ADD COLUMN IF NOT EXISTS tax_exempt TEXT NOT NULL DEFAULT ''")
        cursor.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS customer_snapshot TEXT NOT NULL DEFAULT ''")
        cursor.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS tax_exempt TEXT NOT NULL DEFAULT ''")
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


init_db()


def refresh_jobs_on_startup():
    print("in this func")
    auto_refresh = os.getenv("AUTO_REFRESH_JOBS_ON_STARTUP", "1").strip().lower()
    print(auto_refresh)
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
    year = datetime.now().year
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

    def draw_field(label, value, x, y, w, h, label_width=70):
        pdf.rect(x, y - h, w, h)
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(x + 6, y - 14, label)
        pdf.line(x + label_width, y - h, x + label_width, y)
        pdf.setFont("Helvetica", 10)
        pdf.drawString(x + label_width + 6, y - 14, value or "")

    def draw_ticket_page(copy_title, include_signature_line):
        left = 36
        right = width - 36
        box_w = right - left
        y_top = height - 132

        pdf.setLineWidth(1)
        pdf.rect(left, 30, box_w, height - 70)

        company_start_y = height - 56
        for index, line in enumerate(company_header_lines):
            if index == 0:
                pdf.setFont("Helvetica-Bold", 12)
            else:
                pdf.setFont("Helvetica", 10)
            pdf.drawCentredString(width / 2, company_start_y - (index * 14), line)

        pdf.setFont("Helvetica-Bold", 18)
        pdf.drawCentredString(width / 2, y_top, "DUMP TICKET")
        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawCentredString(width / 2, y_top - 18, copy_title.upper())

        draw_field("Ticket #", ticket["ticket_number"], left + 10, y_top - 32, 250, 24, label_width=65)
        draw_field(
            "Date/Time",
            format_ticket_datetime(ticket["created_at"]),
            left + 270,
            y_top - 32,
            box_w - 280,
            24,
            label_width=72,
        )

        direction_text = "IN  [X]   OUT [ ]" if ticket["direction"] == "IN" else "IN  [ ]   OUT [X]"
        draw_field("Direction", direction_text, left + 10, y_top - 62, box_w - 20, 24, label_width=72)

        draw_field("Job #", ticket["job_code_snapshot"], left + 10, y_top - 92, 180, 24, label_width=45)
        draw_field("Job Name", ticket["job_name_snapshot"], left + 195, y_top - 92, box_w - 205, 24, label_width=65)
        draw_field("Customer", ticket.get("customer_snapshot", ""), left + 10, y_top - 122, box_w - 20, 24, label_width=65)

        draw_field("Truck #", ticket["truck_number_snapshot"], left + 10, y_top - 152, 180, 24, label_width=55)
        draw_field("Material", ticket["material_name_snapshot"], left + 195, y_top - 152, box_w - 205, 24, label_width=55)

        draw_field("Quantity", f"{ticket['quantity']}", left + 10, y_top - 182, 180, 24, label_width=58)
        draw_field("Unit", ticket["unit"], left + 195, y_top - 182, 120, 24, label_width=34)

        notes_y = y_top - 212
        notes_h = 88
        pdf.rect(left + 10, notes_y - notes_h, box_w - 20, notes_h)
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(left + 16, notes_y - 14, "Notes")
        pdf.setFont("Helvetica", 10)
        note_text = ticket.get("notes", "") or ""
        if len(note_text) > 200:
            note_text = note_text[:197] + "..."
        pdf.drawString(left + 70, notes_y - 14, note_text)

        if include_signature_line:
            sig_y = notes_y - notes_h - 36
            draw_field("Driver Name", "", left + 10, sig_y, (box_w - 30) / 2, 24, label_width=70)
            draw_field(
                "Signature",
                "",
                left + 20 + (box_w - 30) / 2,
                sig_y,
                (box_w - 30) / 2,
                24,
                label_width=60,
            )

        pdf.setFont("Helvetica", 8)
        pdf.drawRightString(right - 8, 38, f"Printed: {format_ticket_datetime(datetime.now())}")
        pdf.showPage()

    draw_ticket_page("Driver Copy - Signature Required", True)
    draw_ticket_page("Internal Billing Copy", False)
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
            f"Generated: {datetime.now().strftime('%m-%d-%Y %H:%M')}",
            normal,
        )
    )
    elements.append(Spacer(1, 12))

    # ---- Tickets Table ----
    table_data = [
        ["Ticket #", "Date/Time", "Customer", "Dir", "Material", "Qty", "Unit"]
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
        ])

    ticket_table = Table(
        table_data,
        colWidths=[75, 85, 120, 35, 110, 45, 40],
        repeatRows=1
    )

    ticket_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ("ALIGN", (5, 1), (5, -1), "RIGHT"),
        ("ALIGN", (3, 1), (3, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ]))

    elements.append(ticket_table)
    elements.append(Spacer(1, 20))

    # ---- Totals By Unit ----
    elements.append(Paragraph("Totals By Unit", bold))
    totals_unit_table = Table(
        [["Unit", "Total Quantity"]] +
        [[r["unit"], f"{r['total_quantity']:.2f}"] for r in totals_by_unit],
        colWidths=[100, 120],
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
        [["Material", "Unit", "Total Quantity"]] +
        [
            [
                r["material_name_snapshot"],
                r["unit"],
                f"{r['total_quantity']:.2f}",
            ]
            for r in totals_by_material
        ],
        colWidths=[200, 80, 120],
    )
    totals_mat_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
    ]))
    elements.append(totals_mat_table)

    doc.build(elements)
    buffer.seek(0)
    return buffer.read()

def save_pdf(ticket_number, pdf_bytes):
    year = datetime.now().year
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
    print("Refreshing jobs cache...")
    csv_file = resolve_jobs_csv_path()
    if csv_file is not None:

        now = datetime.now().isoformat(timespec="seconds")
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
        print(f"Jobs cache refreshed from CSV. {synced} rows synced.")

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

    now = datetime.now().isoformat(timespec="seconds")
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
        """
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
    now = datetime.now().isoformat(timespec="seconds")
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

@app.route("/")
def home():
    return redirect(url_for("new_ticket"))


@app.route("/tickets/new", methods=["GET", "POST"])
def new_ticket():
    db = get_db()

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
                cursor.execute("SELECT id, truck_number FROM trucks_main WHERE id = %s", (truck_id,))
                truck = cursor.fetchone()
        if material_id:
            with db.cursor() as cursor:
                cursor.execute("SELECT id, material AS material_name FROM material_price WHERE id = %s", (material_id,))
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

        try:
            ticket_number, ticket_year, seq = next_ticket_number(db)
            if use_now:
                created_at = datetime.now().isoformat(timespec="seconds")
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
                "notes": notes,
            }
            pdf_bytes = to_pdf_bytes(row)
            pdf_path = save_pdf(ticket_number, pdf_bytes)
            print(f"Generated PDF for ticket {ticket_number} at {pdf_path}")

            with db.cursor() as cursor:
                cursor.execute(
                """
                INSERT INTO tickets (
                    ticket_number, ticket_year, ticket_sequence, direction, created_at,
                    job_id, job_code_snapshot, job_name_snapshot, tax_exempt, customer_snapshot, truck_id, truck_number_snapshot,
                    material_id, material_name_snapshot, quantity, unit, notes, pdf_path, pdf_blob
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    notes,
                    pdf_path,
                    pdf_bytes,
                ),
                )
            db.commit()
        except Exception:
            db.rollback()
            raise

        if auto_print:
            try:
                print_pdf_file(pdf_path)
            except Exception as exc:
                flash(f"Ticket saved, but print failed: {exc}", "error")
                return redirect(url_for("new_ticket"))
                # return redirect(url_for("search_tickets", ticket_number=ticket_number))

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
        SELECT id, ticket_number, created_at, direction, job_code_snapshot, tax_exempt, customer_snapshot, truck_number_snapshot, material_name_snapshot
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
        date_to = datetime.now().date().isoformat()
        date_from = (datetime.now().date() - timedelta(days=14)).isoformat()

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
    print(f"Report query WHERE clause: {where_sql} with params {params}")

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
            t.unit
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
        SELECT t.unit, COALESCE(SUM(t.quantity), 0) AS total_quantity
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
        SELECT t.material_name_snapshot, t.unit, COALESCE(SUM(t.quantity), 0) AS total_quantity
        FROM tickets t
        WHERE {where_sql}
        GROUP BY t.material_name_snapshot, t.unit
        ORDER BY t.material_name_snapshot, t.unit
        """,
        tuple(params),
        )
        totals_by_material = cursor.fetchall()

    return render_template(
        "reports.html",
        tickets=tickets,
        offset=offset,
        totals_by_unit=totals_by_unit,
        totals_by_material=totals_by_material,
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
            t.unit
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
        SELECT t.unit, COALESCE(SUM(t.quantity), 0) AS total_quantity
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
        SELECT t.material_name_snapshot, t.unit, COALESCE(SUM(t.quantity), 0) AS total_quantity
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
            ]
        )

    writer.writerow([])
    writer.writerow(["Totals by Unit"])
    writer.writerow(["Unit", "Total Quantity"])
    for total in totals_by_unit:
        writer.writerow([total["unit"], f"{total['total_quantity']:.2f}"])

    writer.writerow([])
    writer.writerow(["Totals by Material"])
    writer.writerow(["Material", "Unit", "Total Quantity"])
    for total in totals_by_material:
        writer.writerow(
            [
                total["material_name_snapshot"],
                total["unit"],
                f"{total['total_quantity']:.2f}",
            ]
        )

    csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
    output.close()
    csv_bytes.seek(0)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(
        csv_bytes,
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"ticket_report_{stamp}.csv",
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
        SELECT unit, SUM(quantity) AS total_quantity
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
        SELECT material_name_snapshot, unit, SUM(quantity) AS total_quantity
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

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_path = REPORT_PDF_DIR / f"ticket_report_{stamp}.pdf"

    with open(pdf_path, "wb") as f:
        f.write(pdf_bytes)

    try:
        print_pdf_file(str(pdf_path))
    except Exception:
        pass

    return send_file(
        pdf_path,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=pdf_path.name,
    )

@app.get("/tickets/<int:ticket_id>/pdf")
def ticket_pdf(ticket_id):
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute("SELECT ticket_number, pdf_blob FROM tickets WHERE id = %s", (ticket_id,))
        row = cursor.fetchone()
    if not row:
        flash("Ticket not found.", "error")
        return redirect(url_for("search_tickets"))

    return send_file(
        io.BytesIO(row["pdf_blob"]),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"{row['ticket_number']}.pdf",
    )


@app.post("/tickets/<int:ticket_id>/print")
def print_ticket(ticket_id):
    db = get_db()
    with db.cursor() as cursor:
        cursor.execute("SELECT ticket_number, pdf_path FROM tickets WHERE id = %s", (ticket_id,))
        row = cursor.fetchone()
    if not row:
        flash("Ticket not found.", "error")
        return redirect(url_for("search_tickets"))

    try:
        print_pdf_file(row["pdf_path"])
        flash(f"Print sent for {row['ticket_number']}.", "success")
    except Exception as exc:
        flash(f"Print failed: {exc}", "error")
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

    with db.cursor() as cursor:
        cursor.execute(
            "SELECT id, truck_number, notes AS description, truck_size, trucking_company AS hauled_by, license_plate, active FROM trucks_main ORDER BY truck_number"
        )
        rows = cursor.fetchall()
    return render_template("admin_trucks.html", trucks=rows)


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


@app.route("/admin/materials", methods=["GET", "POST"])
def admin_materials():
    db = get_db()
    if request.method == "POST":
        material_name = request.form.get("material_name", "").strip()
        if not material_name:
            flash("Material name is required.", "error")
            return redirect(url_for("admin_materials"))
        try:
            active_value = True if is_active_column_boolean(db, "material_price") else 1
            with db.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO material_price (material, active, direction) VALUES (%s, %s, 'IN')",
                    (material_name, active_value),
                )
            db.commit()
            flash("Material added.", "success")
        except IntegrityError:
            db.rollback()
            flash("Material already exists.", "error")
        return redirect(url_for("admin_materials"))

    with db.cursor() as cursor:
        cursor.execute("SELECT id, material AS material_name, axle1, axle2, axle3, axle4, axle5, axle6, axle7, axle8, axle9, active FROM material_price ORDER BY material")
        rows = cursor.fetchall()
    return render_template("admin_materials.html", materials=rows)


@app.post("/admin/materials/<int:material_id>/toggle")
def toggle_material(material_id):
    db = get_db()
    with db.cursor() as cursor:
        if is_active_column_boolean(db, "material_price"):
            cursor.execute(
                "UPDATE material_price SET active = NOT COALESCE(active, FALSE) WHERE id = %s",
                (material_id,),
            )
        else:
            cursor.execute(
                "UPDATE material_price SET active = CASE WHEN COALESCE(active, 0) = 1 THEN 0 ELSE 1 END WHERE id = %s",
                (material_id,),
            )
    db.commit()
    return redirect(url_for("admin_materials"))


if __name__ == "__main__":
    # with app.app_context():
    #     refresh_jobs_on_startup()
    refresh_jobs_on_startup()
    app.run(debug=True, host="127.0.0.1", port=5000)
    