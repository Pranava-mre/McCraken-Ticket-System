# Dump Ticket Tracking App

Local ticket tracking app for dump tickets with:
- ticket entry UI (IN/OUT)
- jobs dropdown from SQL Server (cached locally)
- trucks/materials dropdown from local DB
- PDF ticket generation + storage
- search + reprint
- admin screens for trucks/materials

## Tech Stack
- Python 3.11+
- Flask
- SQLite (local DB)
- pyodbc (SQL Server refresh)
- reportlab (PDF generation)

## Project Structure
- `app.py`: Flask app and all routes
- `schema.sql`: SQLite schema
- `templates/`: HTML screens
- `static/style.css`: basic styles
- `data/tickets.db`: local SQLite DB (auto-created)
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

## Configure SQL Job Refresh
Set environment variables before running:

```powershell
$env:REMOTE_SQL_ODBC_CONNECTION_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=YOURSERVER;Database=YOURDB;Trusted_Connection=yes;"
```

Optional custom query (must return columns aliased exactly as below):
- `job_code`
- `job_name`
- `customer`
- `active`
- `source_updated_at`

Example:
```powershell
$env:JOBS_SQL_QUERY = @"
SELECT
    CAST(JobNumber AS NVARCHAR(100)) AS job_code,
    CAST(JobName AS NVARCHAR(255)) AS job_name,
    CAST(CustomerName AS NVARCHAR(255)) AS customer,
    CAST(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END AS INT) AS active,
    LastUpdated AS source_updated_at
FROM dbo.Jobs
"@
```

## Run
```powershell
python app.py
```
Open: `http://127.0.0.1:5000`

## Workflow
1. Open `New Ticket`.
2. Click `Refresh Jobs Cache` to sync SQL jobs into local `jobs_cache`.
3. Add trucks/materials in admin screens if needed.
4. Enter ticket data and submit.
5. App generates sequential ticket number format: `DT-YYYY-######`.
6. App saves ticket to DB and stores ticket PDF in both:
- `tickets.pdf_blob` (BLOB in DB)
- `tickets.pdf_path` (file path on disk)
7. If auto print is checked, app sends PDF to default Windows printer.
8. Use `Search/Reprint` to find and reprint tickets.

## Notes
- Auto print uses `os.startfile(path, "print")` and is Windows-only.
- If print fails, the ticket still saves and can be reprinted later.
