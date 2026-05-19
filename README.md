# pfc-migrate — Convert compressed JSONL archives to PFC cold storage

[![PyPI](https://img.shields.io/badge/PyPI-pfc--migrate-blue.svg)](https://pypi.org/project/pfc-migrate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![PFC-JSONL](https://img.shields.io/badge/PFC--JSONL-green.svg)](https://github.com/ImpossibleForge/pfc-jsonl)
[![Version](https://img.shields.io/badge/pfc--migrate-v2.1.0-brightgreen.svg)](https://github.com/ImpossibleForge/pfc-migrate/releases)

Convert existing compressed JSONL archives from local disk, S3, Azure, or GCS to PFC format. No intermediate files, no schema changes, no pipelines.

> **Schema conversion (Apache logs, CSV, NDJSON → JSONL → PFC)?**
> Use [pfc-convert](https://github.com/ImpossibleForge/pfc-convert) — it rewrites the data format and compresses in one step.
> pfc-migrate only swaps the compression wrapper (gzip/zstd → .pfc), content is unchanged.

---

## What this does

| Command | What it does |
|---------|-------------|
| `pfc-migrate convert` | Convert gzip/zstd/bzip2/lz4/JSONL files to PFC |
| `pfc-migrate s3` | Convert JSONL archives in S3 in-place |
| `pfc-migrate glacier` | Restore + convert S3 Glacier archives to PFC |
| `pfc-migrate azure` | Convert JSONL archives in Azure Blob Storage |
| `pfc-migrate gcs` | Convert JSONL archives in Google Cloud Storage |

## Works with pfc-convert (pipe mode)

pfc-migrate accepts JSONL from stdin — combine with [pfc-convert](https://github.com/ImpossibleForge/pfc-convert) to convert schema and compress in one streaming pipeline:

```bash
# Apache CLF logs → JSONL → .pfc  (no temp files)
pfc-convert convert access.log.gz --schema apache --stdout \
  | pfc-migrate convert --stdin --out archive.pfc
```

## Automated with pfc-ingest-watchdog

[pfc-ingest-watchdog](https://github.com/ImpossibleForge/pfc-ingest-watchdog) monitors folders or S3 prefixes and triggers pfc-migrate (or pfc-convert) automatically when new files arrive — no manual invocation needed.

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

## Input Formats

| Format | Extension | Extra dependency |
|--------|-----------|-----------------|
| gzip | `.jsonl.gz` | stdlib ✅ |
| bzip2 | `.jsonl.bz2` | stdlib ✅ |
| zstd | `.jsonl.zst` | `pip install pfc-migrate[zstd]` |
| lz4 | `.jsonl.lz4` | `pip install pfc-migrate[lz4]` |
| Plain JSONL | `.jsonl` | stdlib ✅ |

---

## Requirements

**The `pfc_jsonl` binary must be installed on the machine running the conversion:**

```bash
# Linux x64:
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# macOS (Apple Silicon M1–M4):
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-macos-arm64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
```

> **License note:** `pfc_jsonl` is free for personal and open-source use. Commercial use requires a written license — see [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl).

> **macOS Intel (x64):** Binary coming soon. | **Windows:** Use WSL2 or a Linux machine.

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

# Everything
pip install pfc-migrate[all]
```

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
  --tier expedited
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

## Lossless guarantee

Every conversion is verified by full decompression and MD5 check before the output is written. If anything doesn't match, the output file is deleted and the error is reported — the original is never modified. For S3, GCS, and Azure subcommands, `--delete` removes the original only after successful verification.

---

## Migrating from v1.x

In v2.0.0 the database export subcommands (`cratedb`, `questdb`, `timescaledb`, etc.) have been moved to dedicated standalone tools. This keeps pfc-migrate focused on what it was built for: converting compressed file archives.

**Migration:**

```bash
# Before (v1.x)
pfc-migrate cratedb --host localhost --table logs --output logs.pfc

# After (v2.0.0) — install the dedicated tool
pip install pfc-export-cratedb
pfc-export-cratedb --host localhost --table logs --output logs.pfc
```

All flags are identical — it's a drop-in replacement.

---

## Part of the PFC Ecosystem

**[→ View all PFC tools & integrations](https://github.com/ImpossibleForge/pfc-jsonl#ecosystem)**

| Direct integration | Why |
|---|---|
| [pfc-convert](https://github.com/ImpossibleForge/pfc-convert) | Pipe partner — schema conversion (Apache CLF, CSV → JSONL) before or after pfc-migrate |
| [pfc-ingest-watchdog](https://github.com/ImpossibleForge/pfc-ingest-watchdog) | Calls pfc-migrate automatically when new files arrive in folder or S3 |

---

## License

pfc-migrate (this repository) is released under the MIT License — see [LICENSE](LICENSE).

The PFC-JSONL binary (`pfc_jsonl`) is proprietary software — free for personal and open-source use. Commercial use requires a license: [info@impossibleforge.com](mailto:info@impossibleforge.com)