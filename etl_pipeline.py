"""
Purchase Order × Weather ETL pipeline (Python + Snowflake + PostgreSQL).

Extracts supplier, purchase-order, geographic and weather data from five sources,
loads it into Snowflake, and transforms it into one analytical table:
FINAL_PO_INV_SUPPLIER_WEATHER (PO vs. invoice variance + daily high temperature
at the supplier's location on the transaction date).

Usage:
    python etl_pipeline.py                 # run every step
    python etl_pipeline.py --steps weather final
    python etl_pipeline.py --list          # show available steps
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
from pathlib import Path

import psycopg2
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent
SQL_DIR = ROOT / "sql"
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT / "data")).resolve()

XML_FILE = DATA_DIR / "Supplier Transactions XML.xml"
ZCTA_FILE = DATA_DIR / "2021_Gaz_zcta_national.txt"
PO_CSV_DIR = DATA_DIR / "Monthly PO Data"
SUPPLIER_SQL = DATA_DIR / "supplier_case.pgsql"
SUPPLIER_CSV = DATA_DIR / "supplier_case.csv"

STAGE = "pgstage"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
log = logging.getLogger("etl")


# ------------------------------------------------------------------------------
# Connections & helpers
# ------------------------------------------------------------------------------

def snowflake_connection():
    """Connect to Snowflake using credentials from the environment (.env)."""
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role=os.getenv("SNOWFLAKE_ROLE") or None,
    )
    db = os.getenv("SNOWFLAKE_DATABASE", "PROJECTDB")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "PROJECTSCHEMA")
    with conn.cursor() as cs:
        cs.execute(f"CREATE DATABASE IF NOT EXISTS {db}")
        cs.execute(f"USE DATABASE {db}")
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cs.execute(f"USE SCHEMA {schema}")
        cs.execute(f"CREATE STAGE IF NOT EXISTS {STAGE}")
    return conn


def postgres_params() -> dict:
    return dict(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DATABASE", "postgres"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
    )


def run_sql_file(conn, name: str, **params) -> None:
    """Execute every statement in sql/<name>, filling {placeholders} from params."""
    sql = (SQL_DIR / name).read_text()
    if params:
        sql = sql.format(**params)
    conn.execute_string(sql, remove_comments=True)
    log.info("ran sql/%s", name)


def scalar(conn, sql: str):
    with conn.cursor() as cs:
        cs.execute(sql)
        return cs.fetchone()[0]


def require(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"Missing input: {path}  (see data/README.md)")
    return path


def create_csv_format(cs, name: str, delimiter: str = ",") -> None:
    cs.execute(f"""
        CREATE OR REPLACE FILE FORMAT {name}
        TYPE = CSV
        FIELD_DELIMITER = '{delimiter}'
        PARSE_HEADER = TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        TRIM_SPACE = TRUE
        EMPTY_FIELD_AS_NULL = TRUE
        NULL_IF = ('', '\\N', 'NULL')
    """)


def load_with_inferred_schema(cs, table: str, stage_path: str, file_format: str) -> None:
    """Create `table` from the staged file(s)' inferred schema, then COPY the data in."""
    cs.execute(f"""
        CREATE OR REPLACE TABLE {table} USING TEMPLATE (
          SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
          FROM TABLE(INFER_SCHEMA(LOCATION => '@{stage_path}', FILE_FORMAT => '{file_format}'))
        )
    """)
    cs.execute(f"""
        COPY INTO {table}
        FROM @{stage_path}
        FILE_FORMAT = (FORMAT_NAME = {file_format})
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE'
    """)


# ------------------------------------------------------------------------------
# Pipeline steps
# ------------------------------------------------------------------------------

def step_xml(conn) -> None:
    """Extract supplier transactions from XML → VARIANT table → typed view."""
    xml = require(XML_FILE)
    with conn.cursor() as cs:
        cs.execute("CREATE FILE FORMAT IF NOT EXISTS xml_fmt TYPE = XML")
        cs.execute("CREATE OR REPLACE TABLE supplier_transactions_raw (doc VARIANT)")
        cs.execute(f"PUT 'file://{xml}' @{STAGE} AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
        cs.execute(f"""
            COPY INTO supplier_transactions_raw
            FROM @{STAGE}
            FILE_FORMAT = (FORMAT_NAME = xml_fmt)
            PATTERN = '.*Supplier Transactions XML\\.xml'
        """)
    run_sql_file(conn, "01_supplier_transactions_view.sql")
    log.info("supplier_transactions rows: %s", scalar(conn, "SELECT COUNT(*) FROM supplier_transactions"))


def step_zcta(conn) -> None:
    """Load the Census ZCTA Gazetteer (tab-delimited) for ZIP centroids."""
    txt = require(ZCTA_FILE)
    with conn.cursor() as cs:
        create_csv_format(cs, "zcta_fmt", delimiter="\\t")
        cs.execute(f"PUT 'file://{txt}' @{STAGE} AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
        load_with_inferred_schema(cs, "zcta_2021_raw", f"{STAGE}/{txt.name}", "zcta_fmt")
    run_sql_file(conn, "02_zcta_2021_view.sql")
    log.info("zcta_2021 rows: %s", scalar(conn, "SELECT COUNT(*) FROM zcta_2021"))


def step_purchase_orders(conn) -> None:
    """Load every monthly PO CSV into a single table and derive POAmount."""
    require(PO_CSV_DIR)
    with conn.cursor() as cs:
        create_csv_format(cs, "po_csv_fmt")
        cs.execute(f"PUT 'file://{PO_CSV_DIR}/*.csv' @{STAGE}/po_monthly AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
        load_with_inferred_schema(cs, "po_monthly", f"{STAGE}/po_monthly", "po_csv_fmt")
    run_sql_file(conn, "03_po_amount.sql")
    log.info("po_monthly rows: %s", scalar(conn, "SELECT COUNT(*) FROM po_monthly"))


def step_invoices(conn) -> None:
    """Build the typed invoice table and the PO-vs-invoice comparison."""
    run_sql_file(conn, "04_supplier_invoice_data.sql")
    run_sql_file(conn, "05_purchase_orders_and_invoices.sql")
    log.info("purchase_orders_and_invoices rows: %s",
             scalar(conn, "SELECT COUNT(*) FROM purchase_orders_and_invoices"))


def step_suppliers(conn) -> None:
    """Restore supplier data into PostgreSQL, export it to CSV, and load it into Snowflake."""
    pg = postgres_params()
    env = {**os.environ, "PGHOST": pg["host"], "PGPORT": str(pg["port"]), "PGUSER": pg["user"],
           "PGDATABASE": pg["dbname"], "PGPASSWORD": pg["password"]}
    subprocess.run(["psql", "-f", str(require(SUPPLIER_SQL))], check=True, env=env)
    log.info("loaded supplier_case.pgsql into PostgreSQL")

    with psycopg2.connect(**pg) as pg_conn, pg_conn.cursor() as cur, open(SUPPLIER_CSV, "w", newline="") as f:
        cur.copy_expert("COPY public.supplier_case TO STDOUT WITH CSV HEADER", f)
    log.info("exported %s", SUPPLIER_CSV.name)

    with conn.cursor() as cs:
        create_csv_format(cs, "csv_std")
        cs.execute(f"PUT 'file://{SUPPLIER_CSV}' @{STAGE} AUTO_COMPRESS = FALSE OVERWRITE = TRUE")
        load_with_inferred_schema(cs, "supplier_case", f"{STAGE}/{SUPPLIER_CSV.name}", "csv_std")
    log.info("supplier_case rows: %s", scalar(conn, "SELECT COUNT(*) FROM supplier_case"))


def _find_relation(conn, name: str) -> str | None:
    """Locate a Marketplace table/view by name, e.g. NOAA_WEATHER_STATION_INDEX."""
    with conn.cursor() as cs:
        for kind in ("TABLES", "VIEWS"):
            cs.execute(f"SHOW {kind} LIKE '{name}' IN ACCOUNT")
            rows = cs.fetchall()
            cols = [c[0] for c in cs.description]
            if rows:
                r = dict(zip(cols, rows[0]))
                return f'{r["database_name"]}.{r["schema_name"]}.{r["name"]}'
    return None


def _pick_column(conn, fqn: str, candidates: tuple[str, ...]) -> str:
    with conn.cursor() as cs:
        cs.execute(f"SELECT * FROM {fqn} LIMIT 0")
        cols = {c[0].upper() for c in cs.description}
    for c in candidates:
        if c in cols:
            return c
    raise RuntimeError(f"None of {candidates} found in {fqn}; columns are {sorted(cols)}")


def step_weather(conn) -> None:
    """Join supplier ZIPs to the nearest NOAA station (Snowflake Marketplace) for daily highs."""
    stations = os.getenv("NOAA_STATIONS_TABLE") or _find_relation(conn, "NOAA_WEATHER_STATION_INDEX")
    timeseries = os.getenv("NOAA_TIMESERIES_TABLE") or _find_relation(conn, "NOAA_WEATHER_METRICS_TIMESERIES")
    if not stations or not timeseries:
        raise RuntimeError("NOAA Marketplace tables not found. Subscribe to the NOAA weather listing "
                           "or set NOAA_STATIONS_TABLE / NOAA_TIMESERIES_TABLE in .env.")
    date_col = _pick_column(conn, timeseries, ("DATE", "OBSERVATION_DATE", "DAY", "OBS_DATE"))
    high_col = _pick_column(conn, timeseries, ("TMAX", "MAX_TEMP", "DAILY_MAX_TEMPERATURE", "TMAX_F", "TMAX_C"))

    with conn.cursor() as cs:  # clear an older materialized-view version if present
        try:
            cs.execute("DROP MATERIALIZED VIEW IF EXISTS supplier_zip_code_weather")
        except snowflake.connector.errors.ProgrammingError:
            pass
    run_sql_file(conn, "06_supplier_weather.sql", stations_fqn=stations, timeseries_fqn=timeseries,
                 date_col=date_col, high_col=high_col)
    with conn.cursor() as cs:
        cs.execute("SELECT MIN(wx_date), MAX(wx_date), COUNT(*) FROM supplier_zip_code_weather")
        log.info("weather date range & rows: %s", cs.fetchone())


def step_final(conn) -> None:
    """Produce FINAL_PO_INV_SUPPLIER_WEATHER."""
    run_sql_file(conn, "07_final_po_inv_supplier_weather.sql")
    log.info("final_po_inv_supplier_weather rows: %s",
             scalar(conn, "SELECT COUNT(*) FROM final_po_inv_supplier_weather"))


STEPS = {
    "xml": step_xml,
    "zcta": step_zcta,
    "purchase_orders": step_purchase_orders,
    "invoices": step_invoices,
    "suppliers": step_suppliers,
    "weather": step_weather,
    "final": step_final,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--steps", nargs="+", choices=STEPS, help="run only these steps (in pipeline order)")
    parser.add_argument("--list", action="store_true", help="list steps and exit")
    args = parser.parse_args()

    if args.list:
        for name, fn in STEPS.items():
            print(f"{name:<16} {fn.__doc__.strip()}")
        return

    selected = [s for s in STEPS if not args.steps or s in args.steps]
    conn = snowflake_connection()
    try:
        for name in selected:
            log.info("── step: %s", name)
            STEPS[name](conn)
    finally:
        conn.close()
    log.info("pipeline complete")


if __name__ == "__main__":
    main()
