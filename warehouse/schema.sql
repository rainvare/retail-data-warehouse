-- =============================================================================
-- schema.sql  —  Retail Data Warehouse
-- Dimensional model (Star Schema)
-- Compatible with SQLite / PostgreSQL
-- =============================================================================

-- ── Dimension: Date ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_id       INTEGER PRIMARY KEY,   -- YYYYMMDD
    full_date     DATE    NOT NULL,
    day           INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    month_name    TEXT    NOT NULL,
    quarter       INTEGER NOT NULL,
    year          INTEGER NOT NULL,
    week_of_year  INTEGER NOT NULL,
    day_of_week   TEXT    NOT NULL,
    is_weekend    BOOLEAN NOT NULL
);

-- ── Dimension: Customer ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id   INTEGER PRIMARY KEY,
    first_name    TEXT    NOT NULL,
    last_name     TEXT    NOT NULL,
    full_name     TEXT    NOT NULL,
    email         TEXT,
    city          TEXT,
    segment       TEXT    CHECK(segment IN ('Retail','Wholesale','Online'))
);

-- ── Dimension: Product ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_id    INTEGER PRIMARY KEY,
    product_name  TEXT    NOT NULL,
    category      TEXT    NOT NULL,
    unit_cost     REAL    NOT NULL,
    supplier      TEXT
);

-- ── Dimension: Channel ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_name  TEXT    NOT NULL UNIQUE,
    channel_type  TEXT    CHECK(channel_type IN ('digital','physical'))
);

INSERT OR IGNORE INTO dim_channel (channel_name, channel_type) VALUES
    ('online',      'digital'),
    ('mobile_app',  'digital'),
    ('store',       'physical');

-- ── Fact Table: Sales ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id       INTEGER NOT NULL,
    order_item_id  INTEGER NOT NULL,
    date_id        INTEGER NOT NULL  REFERENCES dim_date(date_id),
    customer_id    INTEGER NOT NULL  REFERENCES dim_customer(customer_id),
    product_id     INTEGER NOT NULL  REFERENCES dim_product(product_id),
    channel_id     INTEGER NOT NULL  REFERENCES dim_channel(channel_id),
    -- Measures
    quantity       INTEGER NOT NULL,
    unit_price     REAL    NOT NULL,
    unit_cost      REAL    NOT NULL,
    discount       REAL    NOT NULL DEFAULT 0,
    gross_revenue  REAL    NOT NULL,   -- quantity * unit_price
    net_revenue    REAL    NOT NULL,   -- gross_revenue * (1 - discount)
    cogs           REAL    NOT NULL,   -- quantity * unit_cost
    gross_margin   REAL    NOT NULL,   -- net_revenue - cogs
    status         TEXT    NOT NULL
);

-- ── Indexes ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_date     ON fact_sales(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_product  ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_channel  ON fact_sales(channel_id);
CREATE INDEX IF NOT EXISTS idx_fact_status   ON fact_sales(status);
