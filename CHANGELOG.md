# Changelog — pfc-migrate

All notable changes to pfc-migrate are documented here.

---

## v1.2.0 — 2026-04-20

### Added — QuestDB Direct Export

Export a QuestDB table directly to a `.pfc` archive — no intermediate files.

```bash
pfc-migrate questdb --host quest.example.com --table trades \
  --ts-column timestamp --from-ts "2026-03-01" --to-ts "2026-04-01" \
  --output trades_march.pfc --verbose
```

**Key implementation details:**
- PostgreSQL wire protocol via psycopg2, port 8812 (QuestDB default)
- No `--schema` option — QuestDB has no schema concept, tables are referenced by name only
- Default credentials: `user=admin`, `password=quest`, `dbname=qdb`
- Same batching, time-range filtering, auto-naming, and 0-row guard as the CrateDB subcommand

### Added — new optional dependency group

```bash
pip install pfc-migrate[questdb]   # QuestDB direct export
```

---

## v1.1.0 — 2026-04-14

### Added — CrateDB Direct Export

Export a CrateDB table directly to a `.pfc` archive — no intermediate files, no pipeline setup.

```bash
pfc-migrate cratedb --host crate.example.com --table logs   --ts-column ts --from-ts "2026-03-01" --to-ts "2026-04-01"   --output logs_march.pfc --verbose
```

**Key implementation details:**
- PostgreSQL wire protocol via psycopg2
- `fetchmany(batch_size)` batching — memory-safe, compatible with CrateDB
  (CrateDB does not support named server-side cursors outside transactions)
- `--from-ts` / `--to-ts` date range filtering
- Auto-named output: `<table>_<from>_<to>.pfc` when `--output` is omitted
- 0-row guard: empty date ranges exit cleanly instead of crashing `pfc_jsonl compress`

### Added — new optional dependency group

```bash
pip install pfc-migrate[postgres]   # CrateDB direct export
```

---

## v0.3.2 — 2026-04-08

- Google Cloud Storage (GCS) support (`pfc-migrate gcs`)
- Azure Blob Storage support (`pfc-migrate azure`)
- `DefaultAzureCredential` support for managed identity / `az login`
- Workload Identity support for GKE

## v0.2.0 — 2026-04-08

- Amazon S3 / S3 Glacier support (`pfc-migrate s3`, `pfc-migrate glacier`)
- In-region conversion — no egress charges
- Expedited / Standard / Bulk Glacier retrieval tiers

## v0.1.0 — 2026-04-08

- Initial release
- Local filesystem conversion (`pfc-migrate convert`)
- Supports gzip, bzip2, zstd, lz4, plain JSONL input
- Lossless MD5 verification before writing output
- `--delete` flag to remove originals after successful conversion
