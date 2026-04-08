# pfc-migrate — Convert existing JSONL archives to PFC format

You already have logs stored somewhere — gzip, zstd, bzip2, or lz4 — on disk, on S3, in Glacier, on Azure, or on GCS. Switching to PFC-JSONL means converting them once. This tool does that, wherever your files live.

[![PyPI](https://img.shields.io/badge/PyPI-pfc--migrate-blue.svg)](https://pypi.org/project/pfc-migrate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![PFC-JSONL](https://img.shields.io/badge/PFC--JSONL-v3.4-green.svg)](https://github.com/ImpossibleForge/pfc-jsonl)

---

## Storage Backends

| Backend | Status | Command |
|---------|--------|---------|
| Local filesystem | ✅ v0.1.0 | `pfc-migrate convert` |
| Amazon S3 / S3 Glacier | ✅ v0.2.0 | `pfc-migrate s3` / `pfc-migrate glacier` |
| Azure Blob Storage | ✅ v0.3.0 | `pfc-migrate azure` |
| Google Cloud Storage | ✅ v0.3.0 | `pfc-migrate gcs` |

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

| Tool | 1h query on 30-day archive | Storage savings vs gzip |
|------|----------------------------|-------------------------|
| gzip | Decompress full 30-day file | — |
| zstd | Decompress full 30-day file | — |
| **PFC-JSONL** | **Decompress ~1/720 of the file** | **25% smaller than gzip** |

**~9% compression ratio** on typical JSONL log data (25% smaller than gzip, 37% smaller than zstd).

---

## Zero egress cost

Cloud conversions run **in-region**: download → convert → upload, without ever routing through your laptop or billing you for egress. For Glacier, paying the one-time retrieval cost to convert is worth it — smaller PFC files mean lower future storage, retrieval, and query costs forever.

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

**The `pfc_jsonl` binary must be installed on the machine doing the conversion:**

```bash
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
```

> **Platform:** Linux x86_64. macOS binary coming soon.

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

# Delete originals after successful conversion
pfc-migrate convert --dir /var/log/archive/ --output-dir /var/log/pfc/ --delete
```

**Verbose output:**
```
Found 12 file(s) to convert

  → app_2026-03-01.jsonl.gz  [gz]
     145.3 MB → 13.1 MB  (9.0% of original)  ✓ app_2026-03-01.pfc
  → app_2026-03-02.jsonl.gz  [gz]
     138.7 MB → 12.4 MB  (8.9% of original)  ✓ app_2026-03-02.pfc
  ...

Done: 12 converted, 0 failed
  Input  : 1721.4 MB (decompressed)
  Output : 154.9 MB
  Ratio  : 9.0%
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

# With explicit credentials
pfc-migrate s3 \
  --bucket my-logs --prefix archive/ \
  --out-bucket my-logs-pfc --out-prefix pfc/ \
  --access-key AKIA... --secret-key ... --region us-east-1

# Glacier (Expedited retrieval, converts and re-archives)
pfc-migrate glacier \
  --bucket my-glacier-logs \
  --prefix 2025/ \
  --out-bucket my-glacier-pfc \
  --retrieval-tier Expedited
```

Credentials are read from `~/.aws/credentials`, environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), or passed explicitly via `--access-key` / `--secret-key`.

---

## Usage — Azure Blob Storage

```bash
# Single blob (connection string)
pfc-migrate azure \
  --container my-logs \
  --blob archive/app_2026-03.jsonl.gz \
  --out-container my-logs-pfc \
  --out-prefix converted/ \
  --connection-string "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;"

# All blobs matching a prefix
pfc-migrate azure \
  --container my-logs \
  --prefix archive/2026-03/ \
  --out-container my-logs-pfc \
  --out-prefix converted/2026-03/ \
  --format gz \
  --connection-string "..."

# Using account URL + DefaultAzureCredential (managed identity, az login, etc.)
pfc-migrate azure \
  --container my-logs \
  --prefix archive/ \
  --out-container my-logs-pfc \
  --account-url https://myaccount.blob.core.windows.net
```

`--connection-string` or `--account-url` required. With `--account-url`, `DefaultAzureCredential` is used automatically (managed identity, az login, environment variables).

---

## Usage — Google Cloud Storage

```bash
# Single object
pfc-migrate gcs \
  --bucket my-logs \
  --blob archive/app_2026-03.jsonl.gz \
  --out-bucket my-logs-pfc \
  --out-prefix converted/

# All objects matching a prefix
pfc-migrate gcs \
  --bucket my-logs \
  --prefix archive/2026-03/ \
  --out-bucket my-logs-pfc \
  --out-prefix converted/2026-03/ \
  --format gz \
  --verbose
```

Credentials from `GOOGLE_APPLICATION_CREDENTIALS`, `gcloud auth application-default login`, or Workload Identity on GKE.

---

## Lossless guarantee

Every conversion is verified by full decompression and MD5 check before the output is written. If anything doesn't match, the output file is deleted and the error is reported — the original is never modified unless `--delete` is explicitly passed.

The [full test suite](test_all_backends.py) verifies 4 backends × 4 formats with:
- Sorted MD5 of all 400,000 lines (identical to original ✅)
- DuckDB random access: 39,743 lines in a time window, MD5 identical to original ✅

---

## Related Projects

| Project | Description |
|---------|-------------|
| [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) | Core binary — compress, decompress, query |
| [pfc-duckdb](https://github.com/ImpossibleForge/pfc-duckdb) | DuckDB Community Extension (`INSTALL pfc FROM community`) |
| [pfc-fluentbit](https://github.com/ImpossibleForge/pfc-fluentbit) | Fluent Bit → PFC forwarder for live pipelines |

---

## License

MIT — see [LICENSE](https://github.com/ImpossibleForge/pfc-migrate/blob/main/LICENSE).

*Built by [ImpossibleForge](https://github.com/ImpossibleForge)*
