# pfc-migrate — Move any JSONL log or event data to PFC cold storage

[![PyPI](https://img.shields.io/badge/PyPI-pfc--migrate-blue.svg)](https://pypi.org/project/pfc-migrate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![PFC-JSONL](https://img.shields.io/badge/PFC--JSONL-v3.4-green.svg)](https://github.com/ImpossibleForge/pfc-jsonl)
[![Version](https://img.shields.io/badge/pfc--migrate-v1.1.0-brightgreen.svg)](https://github.com/ImpossibleForge/pfc-migrate/releases)

Export any JSONL data directly to PFC cold storage — or convert existing compressed JSONL archives from local disk, S3, Azure, or GCS. No intermediate files, no schema changes, no pipelines.

---

## What this does

| Command | What it does |
|---------|-------------|
| `pfc-migrate cratedb` | Stream a CrateDB table directly to a `.pfc` archive |
| `pfc-migrate convert` | Convert gzip/zstd/bzip2/lz4/JSONL files to PFC |
| `pfc-migrate s3` | Convert JSONL archives in S3 in-place |
| `pfc-migrate glacier` | Restore + convert S3 Glacier archives to PFC |
| `pfc-migrate azure` | Convert JSONL archives in Azure Blob Storage |
| `pfc-migrate gcs` | Convert JSONL archives in Google Cloud Storage |

---

## Why convert?

Once your archives are in PFC format, DuckDB can query them directly — without decompressing the whole file first:

```sql
INSTALL pfc FROM community;
LOAD pfc;
LOAD json;

-- Query just one hour from a 30-day archive
SELECT line->>'$.level' AS level, line->>'$.message' AS message
FROM read_pfc_jsonl(
    '/var/log/pfc/app_2026-03-01.pfc',
    ts_from = epoch(TIMESTAMPTZ '2026-03-01 14:00:00+00'),
    ts_to   = epoch(TIMESTAMPTZ '2026-03-01 15:00:00+00')
);
```

| Tool | 1h query on 30-day archive | Storage vs gzip |
|------|----------------------------|-----------------|
| gzip | Decompress full 30-day file | — |
| zstd | Decompress full 30-day file | — |
| **PFC-JSONL** | **Decompress ~1/720 of the file** | **25% smaller than gzip** |

**~6–11% compression ratio** on typical JSONL log data (25–40% smaller than gzip).

---

## Zero egress cost

Cloud conversions run **in-region**: download → convert → upload, without ever routing through your laptop or billing for egress.

---

## Input Formats (file conversion)

| Format | Extension | Extra dependency |
|--------|-----------|-----------------|
| gzip | `.jsonl.gz` | stdlib ✅ |
| bzip2 | `.jsonl.bz2` | stdlib ✅ |
| zstd | `.jsonl.zst` | `pip install pfc-migrate[zstd]` |
| lz4 | `.jsonl.lz4` | `pip install pfc-migrate[lz4]` |
| Plain JSONL | `.jsonl` | stdlib ✅ |

---

## Requirements

**The `pfc_jsonl` binary must be installed on the machine running the export:**

```bash
# Linux x64:
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# macOS (Apple Silicon M1–M4):
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-macos-arm64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
```

> **macOS Intel (x64):** Binary coming soon.
> **Windows:** No native binary. Use WSL2 or a Linux machine.

---

## Install

```bash
pip install pfc-migrate

# With zstd support
pip install pfc-migrate[zstd]

# With S3/Glacier support
pip install pfc-migrate[s3]

# With Azure Blob Storage support
pip install pfc-migrate[azure]

# With Google Cloud Storage support
pip install pfc-migrate[gcs]

# For CrateDB direct export
pip install pfc-migrate[postgres]
```

---

## Usage — CrateDB direct export

Stream rows directly from a CrateDB table into a `.pfc` archive. No intermediate files.

```bash
pip install pfc-migrate[postgres]

# Export one week of logs
pfc-migrate cratedb \
  --host crate.example.com \
  --user crate \
  --dbname doc \
  --schema doc \
  --table logs \
  --ts-column ts \
  --from-ts "2026-03-01" --to-ts "2026-03-08" \
  --output logs_2026-03-01.pfc \
  --verbose

# Auto-named output: logs_20260301_20260308.pfc
pfc-migrate cratedb --host localhost --table logs \
  --from-ts "2026-03-01" --to-ts "2026-03-08" --verbose
```

**Verbose output:**
```
  -> Connecting to CrateDB at localhost:5432 (db: doc) ...
  -> Columns (6): ts, level, message, host, service, duration_ms
  -> Streaming rows (batch size: 10,000) ...
     100,000 rows  (17.4 MiB) ...
     200,000 rows  (34.8 MiB) ...
  -> Exported 250,000 rows  (43.7 MiB JSONL)
  -> Compressing with pfc_jsonl ...
  ✓ 250,000 rows  |  JSONL 43.7 MiB  ->  PFC 2.6 MiB  (5.9%)  ->  logs_20260301_20260308.pfc
```

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | localhost | CrateDB host |
| `--port` | 5432 | PostgreSQL wire port |
| `--user` | crate | Username |
| `--password` | _(empty)_ | Password |
| `--dbname` | doc | Database name |
| `--schema` | doc | Schema name |
| `--table` | **required** | Table to export |
| `--ts-column` | None | Timestamp column for WHERE filter and ORDER BY |
| `--from-ts` | None | Start of range (inclusive, ISO 8601) |
| `--to-ts` | None | End of range (exclusive, ISO 8601) |
| `--batch-size` | 10000 | Rows per fetch (memory-safe batching) |
| `--output` | _(auto)_ | Output `.pfc` file |
| `--verbose` | false | Show row progress and size stats |

---

## Usage — Local filesystem

```bash
# Single file (output auto-named: logs.pfc + logs.pfc.bidx)
pfc-migrate convert logs.jsonl.gz

# Explicit output
pfc-migrate convert logs.jsonl.gz logs.pfc

# Entire directory
pfc-migrate convert --dir /var/log/archive/ --output-dir /var/log/pfc/

# Recursive + verbose
pfc-migrate convert --dir /mnt/logs/ -r -v

```

---

## Usage — Amazon S3 / S3 Glacier

Conversion happens in-region (download to temp dir → convert → upload). No egress charges.

```bash
# Single object
pfc-migrate s3 \
  --bucket my-logs \
  --key archive/app_2026-03.jsonl.gz \
  --out-bucket my-logs-pfc \
  --out-prefix converted/

# All objects matching a prefix
pfc-migrate s3 \
  --bucket my-logs \
  --prefix archive/2026-03/ \
  --out-bucket my-logs-pfc \
  --out-prefix converted/2026-03/ \
  --format gz \
  --verbose

# Glacier (Expedited retrieval)
pfc-migrate glacier \
  --bucket my-glacier-logs \
  --prefix 2025/ \
  --out-bucket my-glacier-pfc \
  --retrieval-tier Expedited
```

---

## Usage — Azure Blob Storage

```bash
# All blobs matching a prefix
pfc-migrate azure \
  --container my-logs \
  --prefix archive/2026-03/ \
  --out-container my-logs-pfc \
  --connection-string "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;"
```

---

## Usage — Google Cloud Storage

```bash
# All objects matching a prefix
pfc-migrate gcs \
  --bucket my-logs \
  --prefix archive/2026-03/ \
  --out-bucket my-logs-pfc \
  --verbose
```

---

## Hybrid queries: CrateDB live + PFC cold storage

Query CrateDB live data and cold PFC archives in a single DuckDB SQL statement:

```python
import duckdb, psycopg2

con = duckdb.connect()
con.execute("INSTALL pfc FROM community; LOAD pfc; LOAD json;")

# Register CrateDB live data as a view
cratedb_conn = psycopg2.connect(host="localhost", user="crate", dbname="doc")
live_data = cratedb_conn.cursor()
live_data.execute("SELECT * FROM logs WHERE ts >= '2026-04-01'")
con.register("live_logs", live_data.fetchall())

# Query cold PFC archives + hot live data in one SQL
result = con.execute("""
    SELECT ts, level, message
    FROM pfc_scan([
        '/archives/logs_2026-01.pfc',
        '/archives/logs_2026-02.pfc',
        '/archives/logs_2026-03.pfc'
    ])
    UNION ALL
    SELECT ts, level, message FROM live_logs
    ORDER BY ts
""").fetchall()
```

See [examples/cratedb_archive_explorer.py](examples/cratedb_archive_explorer.py) for a complete demo.

---

## Lossless guarantee

Every conversion is verified by full decompression and MD5 check before output is written. If anything doesn't match, the output file is deleted and the error is reported — the original is never modified. For S3, GCS, and Azure subcommands, `--delete` removes the original cloud object only after successful verification.

---

## Related Projects

| Project | Description |
|---------|-------------|
| [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) | Core binary — compress, decompress, query |
| [pfc-duckdb](https://github.com/ImpossibleForge/pfc-duckdb) | DuckDB Community Extension (`INSTALL pfc FROM community`) |
| [pfc-fluentbit](https://github.com/ImpossibleForge/pfc-fluentbit) | Fluent Bit -> PFC forwarder for live pipelines |
| [pfc-archiver](https://github.com/ImpossibleForge/pfc-archiver) | Autonomous daemon: archive old CrateDB partitions automatically |

---

## License

MIT — see [LICENSE](https://github.com/ImpossibleForge/pfc-migrate/blob/main/LICENSE).

*Built by [ImpossibleForge](https://github.com/ImpossibleForge)*
