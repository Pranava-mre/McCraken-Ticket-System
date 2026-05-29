# Dump Ticket Tracking App

Local ticket tracking app for dump tickets with:
- ticket entry UI (IN/OUT)
- jobs dropdown from SQL Server (cached locally)
- trucks/materials dropdown from PostgreSQL
- customer admin screen and dropdown-backed selection
- PDF ticket generation + storage
- search + reprint
- admin screens for customers/trucks/materials

## Tech Stack
- Python 3.11+
- Flask
- PostgreSQL (Azure Database for PostgreSQL supported)
- psycopg2
- pyodbc (SQL Server refresh)
- reportlab (PDF generation)
- python-dotenv

## Project Structure
- `app.py`: Flask app and all routes
- `schema.sql`: PostgreSQL schema
- `templates/`: HTML screens
- `static/style.css`: basic styles
- `.env`: environment variables for DB + app config
- `tickets_pdf/<year>/`: generated ticket PDFs

## Install
1. Create and activate a virtual environment.
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```
2. Install dependencies.
```powershell
pip install -r requirements.txt
```

3. Configure environment variables in `.env`.

Use either `DATABASE_URL` or `PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD` values.

Example Azure connection:
```env
DATABASE_URL=postgresql://username:password@your-server.postgres.database.azure.com:5432/your_database?sslmode=require
```

Or separate variables:
```env
PGHOST=your-server.postgres.database.azure.com
PGPORT=5432
PGDATABASE=your_database
PGUSER=your_user@your-server
PGPASSWORD=your_password
PGSSLMODE=require
MATERIAL_ADMIN_PASSWORD=your_material_admin_password

# Optional: where generated PDFs are stored
# Use absolute paths for OneDrive/shared folders.
TICKETS_PDF_DIR=C:/Users/your_user/OneDrive - YourCompany/Shared/TicketSystem/tickets_pdf
REPORT_PDF_DIR=C:/Users/your_user/OneDrive - YourCompany/Shared/TicketSystem/reports_pdf
```

## Configure SQL Job Refresh (Optional)
Set optional environment variables in `.env`:

```env
REMOTE_SQL_ODBC_CONNECTION_STRING=Driver={ODBC Driver 17 for SQL Server};Server=YOURSERVER;Database=YOURDB;Trusted_Connection=yes;
```

Optional custom query (must return columns aliased exactly as below):
- `job_code`
- `job_name`
- `customer`
- `active`
- `source_updated_at`

Example:
```sql
SELECT
    CAST(JobNumber AS NVARCHAR(100)) AS job_code,
    CAST(JobName AS NVARCHAR(255)) AS job_name,
    CAST(CustomerName AS NVARCHAR(255)) AS customer,
    CAST(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END AS INT) AS active,
    LastUpdated AS source_updated_at
FROM dbo.Jobs
```

## Configure CSV Job Refresh From Google Drive (Azure Friendly)
If your app is deployed to Azure and must pull jobs from Google Drive, set these environment variables:

```env
JOBS_CSV_URL=https://drive.google.com/file/d/YOUR_FILE_ID/view?usp=sharing
JOBS_CSV_CACHE_PATH=data/jobs_remote_cache.csv
```

How it works:
- On each refresh, app downloads CSV from `JOBS_CSV_URL`.
- Google Drive share links are automatically converted to direct download format.
- If download fails, app falls back to `JOBS_CSV_CACHE_PATH` if that file already exists.
- If no cache exists and download fails, refresh fails with a clear error.

Google Drive requirements:
- File must be shared so the Azure app can access it without interactive login.
- Best setting is "Anyone with the link" viewer access.
- Use the same file (replace content in Drive) instead of changing file ID each time.

Priority order for job sources:
1. `JOBS_CSV_URL` (remote download)
2. `JOBS_CSV_PATH` (local file path)
3. `data/jobs.csv`
4. `G:\My Drive\Jobs Master ALL.csv`
5. SQL ODBC source (`REMOTE_SQL_ODBC_CONNECTION_STRING`)

## Configure Azure Blob Storage For PDFs (Recommended For Deployment)
For Azure App Service, use Blob Storage instead of local/OneDrive paths.

Add these settings in your Azure App Service Configuration (or local `.env`):

```env
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net
AZURE_STORAGE_CONTAINER=ticket-pdfs
AZURE_TICKETS_BLOB_PREFIX=tickets
AZURE_REPORTS_BLOB_PREFIX=reports
AZURE_DOWNLOADS_BLOB_PREFIX=downloads
```

Behavior:
- Ticket PDFs are uploaded to `tickets/<year>/<ticket_number>.pdf`.
- Report PDFs are uploaded to `reports/ticket_report_YYYYMMDD_HHMMSS.pdf`.
- Every CSV/PDF download also writes an audit copy to `downloads/<category>/...`.
- If Blob is not configured, app falls back to local folders (`TICKETS_PDF_DIR` and `REPORT_PDF_DIR`).

## Run
```powershell
python app.py
```
Open: `http://127.0.0.1:5000`

## Azure Deployment (App Service Linux)
- This repo includes a `Procfile` for production startup via Gunicorn:
    - `web: gunicorn --bind 0.0.0.0:${PORT:-8000} --workers 2 --timeout 120 app:app`
- In Azure App Service Configuration, set required app settings from `.env.example`.
- Keep `FLASK_DEBUG=0` in production.

## Security Notes
- Never commit `.env` to source control.
- Rotate credentials immediately if a secret has been committed in the past.

## RFID Truck Notification Webhook
You can call a webhook from an external/local RFID script when a known truck is detected.

Environment variable (recommended):
```env
RFID_EVENT_API_KEY=your_long_random_key
```

Webhook endpoint:
```text
POST /api/notifications/truck-seen
```

Authentication:
- If `RFID_EVENT_API_KEY` is set, send header `X-API-Key: <key>` (or `?api_key=...`).
- If `RFID_EVENT_API_KEY` is not set, only localhost calls are accepted (`127.0.0.1` / `::1`).

Required payload:
- `truck_number` (string)

Optional payload:
- `source` (string, example: `gate-reader-1`)
- `message` (string, custom notification text)
- `detected_at` (ISO timestamp, example: `2026-05-15T10:23:00Z`)

Example request:
```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/api/notifications/truck-seen" `
    -Headers @{"X-API-Key"="your_long_random_key"} `
    -ContentType "application/json" `
    -Body '{"truck_number":"TRK-102","source":"rfid-gate-1"}'
```

Behavior:
- The truck number is validated against active trucks in `trucks_main`.
- If known, a notification is stored and shown in the web app as a toast alert for logged-in users.
- The UI polls `GET /api/notifications/poll` every 5 seconds.

## Daily Sales PDF APIs (Azure Blob + Temporary URL)
Set these environment variables:

```env
CREDIT_CARD_REPORT_API_KEY=your_long_random_key
CREDIT_CARD_CUSTOMER_MATCH=credit card
CREDIT_CARD_REPORT_SAS_MINUTES=180
```

Authentication:
- Send `X-API-Key: <CREDIT_CARD_REPORT_API_KEY>` (or `?api_key=...`).

Request parameters (JSON body or query string):
- `report_date` (optional, format `YYYY-MM-DD`, default is today in app timezone)
- `sas_minutes` (optional integer, default from `CREDIT_CARD_REPORT_SAS_MINUTES`)

1. Credit-card-only daily sales report PDF:
- `POST /api/reports/credit-card/daily`
- `POST /reports/credit-card/daily`

2. Non-credit-card daily sales report PDF (grouped by customer):
- `POST /api/reports/non-credit-card/daily`
- `POST /reports/non-credit-card/daily`

Both endpoints:
- Generate a PDF.
- Upload to Azure Blob Storage under the reports prefix.
- Return a temporary SAS URL (`sas_url`) plus expiry timestamp.

Example call:
```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:5000/api/reports/non-credit-card/daily" `
    -Headers @{"X-API-Key"="your_long_random_key"} `
    -ContentType "application/json" `
    -Body '{"report_date":"2026-05-27","sas_minutes":180}'
```

## Workflow
1. Open `New Ticket`.
2. Click `Refresh Jobs Cache` to sync SQL jobs into PostgreSQL `jobs_cache`.
3. Add customers, trucks, and materials in admin screens if needed.
4. Enter ticket data and submit.
5. App generates sequential ticket number format: `DT-YYYY-######`.
6. App saves ticket to PostgreSQL and stores ticket PDF in both:
- `tickets.pdf_blob` (BLOB in DB)
- `tickets.pdf_path` (file path on disk)
7. If auto print is checked, app sends PDF to default Windows printer.
8. Use `Search/Reprint` to find and reprint tickets.

## Notes
- Auto print uses `os.startfile(path, "print")` and is Windows-only.
- If print fails, the ticket still saves and can be reprinted later.
