CREATE TABLE IF NOT EXISTS jobs_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_code TEXT NOT NULL UNIQUE,
    job_name TEXT NOT NULL,
    customer TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    source_updated_at TEXT,
    refreshed_at TEXT
);

CREATE TABLE IF NOT EXISTS trucks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    truck_number TEXT NOT NULL UNIQUE,
    description TEXT,
    truck_size TEXT NOT NULL DEFAULT '',
    hauled_by TEXT NOT NULL DEFAULT '',
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS materials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    material_name TEXT NOT NULL UNIQUE,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS ticket_sequence (
    ticket_year INTEGER PRIMARY KEY,
    last_value INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS material_price (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cat INTEGER NOT NULL,
    material TEXT NOT NULL UNIQUE,
    direction TEXT NOT NULL CHECK(direction IN ('IN', 'OUT')),
    axle1 REAL ,
    axle2 REAL ,
    axle3 REAL ,
    axle4 REAL ,
    axle5 REAL ,
    axle6 REAL ,
    axle7 REAL ,
    axle8 REAL ,
    axle9 REAL ,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name TEXT,
    full_address TEXT,
    contact_person TEXT,
    phone_number TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS trucks_main(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    truck_number TEXT NOT NULL UNIQUE,
    trucking_company TEXT NOT NULL DEFAULT '',
    notes TEXT,
    truck_size TEXT NOT NULL DEFAULT '',
    phone TEXT,
    license_plate TEXT,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_number TEXT NOT NULL UNIQUE,
    ticket_year INTEGER NOT NULL,
    ticket_sequence INTEGER NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('IN', 'OUT')),
    created_at TEXT NOT NULL,
    job_id INTEGER,
    job_code_snapshot TEXT NOT NULL,
    job_name_snapshot TEXT NOT NULL,
    customer_snapshot TEXT NOT NULL DEFAULT '',
    truck_id INTEGER,
    truck_number_snapshot TEXT NOT NULL,
    material_id INTEGER,
    material_name_snapshot TEXT NOT NULL,
    quantity REAL NOT NULL,
    unit TEXT NOT NULL,
    notes TEXT,
    pdf_path TEXT NOT NULL,
    pdf_blob BLOB NOT NULL,
    FOREIGN KEY(job_id) REFERENCES jobs_cache(id),
    FOREIGN KEY(truck_id) REFERENCES trucks(id),
    FOREIGN KEY(material_id) REFERENCES materials(id)
);

CREATE INDEX IF NOT EXISTS idx_tickets_created_at ON tickets(created_at);
CREATE INDEX IF NOT EXISTS idx_tickets_ticket_number ON tickets(ticket_number);
CREATE INDEX IF NOT EXISTS idx_tickets_job_code ON tickets(job_code_snapshot);
CREATE INDEX IF NOT EXISTS idx_tickets_truck_number ON tickets(truck_number_snapshot);
