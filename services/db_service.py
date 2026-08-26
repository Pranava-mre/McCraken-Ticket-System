import os
import psycopg2
from psycopg2.extras import RealDictCursor

PG_CONNECT_TIMEOUT = 10
from pathlib import Path
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ENV_PATH)
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