# Dump Ticket Tracking App

Local ticket tracking app for dump tickets with:
- ticket entry UI (IN/OUT)
- jobs dropdown from SQL Server (cached locally)
- trucks/materials dropdown from PostgreSQL
- PDF ticket generation + storage
- search + reprint
- admin screens for trucks/materials

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

## Run
```powershell
python app.py
```
Open: `http://127.0.0.1:5000`

## Workflow
1. Open `New Ticket`.
2. Click `Refresh Jobs Cache` to sync SQL jobs into PostgreSQL `jobs_cache`.
3. Add trucks/materials in admin screens if needed.
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
