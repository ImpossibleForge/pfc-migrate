# pfc-migrate — Convert compressed JSONL archives to PFC format

You already have logs on disk — compressed with gzip, zstd, or bzip2. Switching to PFC-JSONL means converting them. This tool does that, in one command.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![PFC-JSONL](https://img.shields.io/badge/PFC--JSONL-v3.4+-green.svg)](https://github.com/ImpossibleForge/pfc-jsonl)

---

## Supported Formats

| Input | Extension | Requires |
|-------|-----------|----------|
| gzip | `.jsonl.gz` | stdlib ✅ |
| bzip2 | `.jsonl.bz2` | stdlib ✅ |
| zstd | `.jsonl.zst` | `pip install zstandard` |
| lz4 | `.jsonl.lz4` | `pip install lz4` |
| Plain JSONL | `.jsonl` | stdlib ✅ |

---

## Requirements

**The `pfc_jsonl` binary must be installed on your machine:**

```bash
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
```

> Platform: Linux x86_64. macOS binary coming soon.

---

## Install

```bash
pip install pfc-migrate

# With zstd support
pip install pfc-migrate[zstd]

# With all optional formats
pip install pfc-migrate[all]
```

Or run directly without installing:
```bash
python pfc_migrate.py convert logs.jsonl.gz logs.pfc
```

---

## Usage

### Single file

```bash
# Auto-detect format from extension
pfc-migrate convert logs.jsonl.gz logs.pfc

# Output name auto-generated (logs.pfc)
pfc-migrate convert logs.jsonl.gz

# Force format
pfc-migrate convert logs.jsonl.gz logs.pfc --format gz
```

### Entire directory

```bash
# Convert all JSONL archives in /var/log/archive/
pfc-migrate convert --dir /var/log/archive/ --output-dir /var/log/pfc/

# Recursive + verbose
pfc-migrate convert --dir /mnt/logs/ -r -v

# Force format for entire directory
pfc-migrate convert --dir /var/log/ --format gz -v
```

### Verbose output

```bash
pfc-migrate convert --dir /var/log/archive/ -v

# Output:
# Found 12 file(s) to convert
#
#   → app_2026-03-01.jsonl.gz  [gz]
#      145.3 MB → 13.1 MB  (9.0% of input)  ✓ app_2026-03-01.pfc
#   → app_2026-03-02.jsonl.gz  [gz]
#      138.7 MB → 12.4 MB  (8.9% of input)  ✓ app_2026-03-02.pfc
#   ...
#
# Done: 12 converted, 0 failed
#   Input  : 1721.4 MB
#   Output : 154.9 MB  (9.0% of input)
#   Saved  : 1566.5 MB
```

---

## Why convert to PFC?

Once your archives are in PFC format, you can query them directly in DuckDB — without decompressing the whole file first:

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

Only the relevant blocks are decompressed. A 1-hour query on a 30-day archive reads ~1/720 of the file.

---

## Roadmap

| Stage | Status |
|-------|--------|
| **Stage 1** — Local (gzip, zstd, bzip2, lz4) | ✅ **v0.1.0** |
| **Stage 2** — Amazon S3 + Glacier (in-region, no egress) | 🔲 Planned |
| **Stage 3** — Azure Blob Storage + GCS | 🔲 Planned |

---

## Related Projects

| Project | Description |
|---------|-------------|
| [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) | Core binary — compress, decompress, query |
| [pfc-duckdb](https://github.com/ImpossibleForge/pfc-duckdb) | DuckDB Community Extension |
| [pfc-fluentbit](https://github.com/ImpossibleForge/pfc-fluentbit) | Fluent Bit → PFC forwarder |

---

## License

MIT — see [LICENSE](LICENSE).

*Built by [ImpossibleForge](https://github.com/ImpossibleForge)*
