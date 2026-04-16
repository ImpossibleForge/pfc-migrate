#!/usr/bin/env python3
"""
cratedb_archive_explorer.py — Hybrid Archive Query Demo
========================================================

Demonstrates querying LIVE CrateDB data and COLD PFC archives
in a single SQL statement via DuckDB.

No data movement required. No extra indexing. No cloud query fees.

Architecture:
  ┌──────────────────┐     ┌─────────────────────────────────┐
  │  CrateDB (live)  │     │  PFC Archive on S3 / local disk │
  │  "hot" data      │     │  "cold" data (older partitions) │
  │  last 30 days    │     │  months / years of history      │
  └────────┬─────────┘     └────────────────┬────────────────┘
           │  psycopg2                       │  pfc_duckdb extension
           │  (PostgreSQL wire protocol)     │  (block-indexed read)
           └──────────────┬─────────────────┘
                          │
                    DuckDB (local)
                   one SQL query
                          │
                    Your result


Requirements:
  pip install duckdb psycopg2-binary pandas

Usage:
  python cratedb_archive_explorer.py

  Or run cells interactively in a Jupyter notebook.
"""

# ---------------------------------------------------------------------------
# CONFIG — adjust to your environment
# ---------------------------------------------------------------------------

CRATEDB_HOST     = "localhost"
CRATEDB_PORT     = 5432
CRATEDB_USER     = "crate"
CRATEDB_PASSWORD = ""
CRATEDB_DBNAME   = "doc"
CRATEDB_SCHEMA   = "doc"
CRATEDB_TABLE    = "logs"           # live "hot" table in CrateDB
CRATEDB_TS_COL   = "timestamp"      # timestamp column name

# PFC archive files (local path or s3:// path when using pfc_duckdb extension)
# These are the "cold" archives produced by:
#   pfc-migrate cratedb --host ... --table logs --output ...
PFC_ARCHIVES = [
    "logs_20240101_20240201.pfc",
    "logs_20231201_20240101.pfc",
    # add more archive files here
]

# Date boundary: everything BEFORE this date is in the archive,
# everything AFTER (or equal) is in live CrateDB
ARCHIVE_CUTOFF = "2024-02-01T00:00:00"  # ISO 8601


# ---------------------------------------------------------------------------
# Step 1: Load CrateDB "hot" data into DuckDB as an in-memory table
# ---------------------------------------------------------------------------

def load_cratedb_into_duckdb(duckdb_conn, cratedb_cfg: dict, cutoff: str):
    """
    Pull recent rows from CrateDB (after cutoff) into a DuckDB in-memory table.

    We load only the hot window — not the full table — so this is fast.
    For very large hot windows, consider loading only the columns you need.
    """
    import psycopg2
    import json

    print(f"[1/3] Connecting to CrateDB ({cratedb_cfg['host']}:{cratedb_cfg['port']}) ...")

    conn = psycopg2.connect(
        host=cratedb_cfg["host"],
        port=cratedb_cfg["port"],
        user=cratedb_cfg["user"],
        password=cratedb_cfg["password"],
        dbname=cratedb_cfg["dbname"],
        connect_timeout=30,
    )
    conn.autocommit = True

    schema = cratedb_cfg["schema"]
    table  = cratedb_cfg["table"]
    ts_col = cratedb_cfg["ts_column"]

    query = (
        f'SELECT * FROM "{schema}"."{table}" '
        f'WHERE "{ts_col}" >= %s '
        f'ORDER BY "{ts_col}"'
    )

    print(f"[1/3] Fetching hot data (since {cutoff}) ...")
    cur = conn.cursor(name="explorer_hot")
    cur.itersize = 10_000
    cur.execute(query, (cutoff,))

    col_names  = [desc[0] for desc in cur.description]
    col_clause = ", ".join(f'"{c}"' for c in col_names)

    rows = []
    for raw_row in cur:
        row_dict = {}
        for col, val in zip(col_names, raw_row):
            from datetime import datetime
            if isinstance(val, datetime):
                val = val.isoformat()
            elif isinstance(val, bytes):
                val = val.hex()
            row_dict[col] = val
        rows.append(row_dict)

    cur.close()
    conn.close()
    print(f"[1/3] Loaded {len(rows):,} rows from CrateDB hot table.")

    if not rows:
        print("     (no hot rows — cutoff may be in the future)")
        # Register empty table so queries don't fail
        duckdb_conn.execute(
            f"CREATE OR REPLACE TABLE cratedb_hot AS "
            f"SELECT * FROM (VALUES (NULL)) t(placeholder) WHERE FALSE"
        )
        return

    # Register as a DuckDB relation from Python list-of-dicts
    import duckdb
    rel = duckdb_conn.read_json(
        json.dumps(rows).encode(),   # DuckDB reads JSON bytes
    )
    duckdb_conn.register("cratedb_hot", rel)
    print(f"[1/3] Registered as DuckDB table: cratedb_hot")


# ---------------------------------------------------------------------------
# Step 2: Register PFC archive files with DuckDB (via pfc_duckdb extension)
# ---------------------------------------------------------------------------

def load_pfc_archives_into_duckdb(duckdb_conn, archive_files: list):
    """
    Register PFC archive files as a DuckDB table using the pfc_duckdb extension.

    The extension loads ONLY the 32 MiB blocks that match a query predicate —
    so even a 100 GB archive is queried in seconds when you filter by time.
    """
    print(f"\n[2/3] Loading {len(archive_files)} PFC archive(s) ...")

    try:
        duckdb_conn.execute("LOAD pfc_duckdb;")
    except Exception as e:
        print(
            "ERROR: pfc_duckdb extension not loaded.\n"
            "Install: https://github.com/ImpossibleForge/pfc-duckdb\n"
            f"Detail: {e}"
        )
        raise

    archive_list = ", ".join(f"'{f}'" for f in archive_files)

    # pfc_scan() = DuckDB table function from the pfc_duckdb extension
    # It reads only the blocks relevant to any WHERE clause on the timestamp column
    duckdb_conn.execute(f"""
        CREATE OR REPLACE VIEW pfc_cold AS
        SELECT * FROM pfc_scan([{archive_list}])
    """)

    row_count = duckdb_conn.execute("SELECT COUNT(*) FROM pfc_cold").fetchone()[0]
    print(f"[2/3] PFC cold archive: {row_count:,} total rows visible to DuckDB")
    print(f"      (Only blocks matching your WHERE clause will be loaded at query time)")


# ---------------------------------------------------------------------------
# Step 3: Run a UNIFIED query across hot + cold data
# ---------------------------------------------------------------------------

def run_hybrid_query(duckdb_conn, ts_col: str, query_from: str, query_to: str):
    """
    Example: count events per hour across the full time range,
    spanning both the PFC archive and the live CrateDB data.

    Replace this with any SQL you need.
    """
    print(f"\n[3/3] Running hybrid query: {query_from} → {query_to}")
    print("      (UNION of CrateDB hot + PFC cold — one SQL, two sources)")

    sql = f"""
        SELECT
            DATE_TRUNC('hour', CAST("{ts_col}" AS TIMESTAMP)) AS hour,
            COUNT(*)                                           AS events,
            'hot'                                             AS source
        FROM cratedb_hot
        WHERE "{ts_col}" BETWEEN '{query_from}' AND '{query_to}'
        GROUP BY hour, source

        UNION ALL

        SELECT
            DATE_TRUNC('hour', CAST("{ts_col}" AS TIMESTAMP)) AS hour,
            COUNT(*)                                           AS events,
            'cold'                                            AS source
        FROM pfc_cold
        WHERE "{ts_col}" BETWEEN '{query_from}' AND '{query_to}'
        GROUP BY hour, source

        ORDER BY hour, source
    """

    result = duckdb_conn.execute(sql).df()  # returns pandas DataFrame
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    try:
        import duckdb
    except ImportError:
        print("ERROR: duckdb required. Install: pip install duckdb")
        raise

    try:
        import pandas as pd
    except ImportError:
        print("WARNING: pandas not installed — result will be a list, not DataFrame")

    print("=" * 60)
    print("  PFC + CrateDB Archive Explorer — Hybrid Query Demo")
    print("=" * 60)

    # In-memory DuckDB (no file needed)
    con = duckdb.connect(database=":memory:")

    cratedb_cfg = {
        "host":      CRATEDB_HOST,
        "port":      CRATEDB_PORT,
        "user":      CRATEDB_USER,
        "password":  CRATEDB_PASSWORD,
        "dbname":    CRATEDB_DBNAME,
        "schema":    CRATEDB_SCHEMA,
        "table":     CRATEDB_TABLE,
        "ts_column": CRATEDB_TS_COL,
    }

    # 1 — Load hot data from CrateDB
    load_cratedb_into_duckdb(con, cratedb_cfg, cutoff=ARCHIVE_CUTOFF)

    # 2 — Register PFC archives
    load_pfc_archives_into_duckdb(con, PFC_ARCHIVES)

    # 3 — Run hybrid query
    # Example: last 90 days across both sources
    result = run_hybrid_query(
        con,
        ts_col     = CRATEDB_TS_COL,
        query_from = "2023-12-01T00:00:00",
        query_to   = "2024-03-01T00:00:00",
    )

    print("\n" + "=" * 60)
    print("  RESULT — Events per hour (hot + cold combined):")
    print("=" * 60)
    print(result.to_string(index=False))
    print(f"\nTotal rows in result: {len(result):,}")
    print("\nDone. No data left your infrastructure. No scan fees.")


if __name__ == "__main__":
    main()
