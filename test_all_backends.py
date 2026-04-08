#!/usr/bin/env python3
"""
pfc-migrate — Complete Backend + Random Access Test
=====================================================
Tests ALL storage backends:
  - Local filesystem  (Stage 1)
  - S3 / MinIO        (Stage 2)
  - Azure / Azurite   (Stage 3)
  - GCS / fake-gcs    (Stage 3)

For each backend and each compression format (gz, bz2, zst, lz4):
  1. Upload compressed file to storage
  2. Convert via pfc-migrate → .pfc (in-place)
  3. Download .pfc + .bidx
  4. Lossless check: full decompress + MD5 vs original
  5. Random access check: DuckDB ts_from/ts_to query vs original filtered lines

This proves: convert once, query only what you need — lossless and fast.
"""

import bz2
import gzip
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import urllib3
from pathlib import Path

urllib3.disable_warnings()  # suppress SSL warnings for local emulators

# -----------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------

PFC_BINARY  = "/usr/local/bin/pfc_jsonl"
DUCKDB_BIN  = "/usr/local/bin/duckdb"
MIGRATE_PY  = "/root/pfc_migrate.py"
TEST_DIR    = Path("/root/pfc_all_backend_test")

# MinIO (S3)
S3_ENDPOINT   = "http://localhost:9000"
S3_ACCESS     = "minioadmin"
S3_SECRET     = "minioadmin"
S3_BUCKET     = "pfc-all-test"
S3_REGION     = "us-east-1"

# Azurite (Azure)
AZURE_CONN    = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
AZURE_CONTAINER = "pfc-test"

# fake-gcs
GCS_ENDPOINT  = "http://localhost:4443"
GCS_BUCKET    = "pfc-gcs-test"

FORMATS = ["gz", "bz2", "zst", "lz4"]

# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def mb(path):
    return Path(path).stat().st_size / 1_048_576

def banner(text, level=1):
    if level == 1:
        print(f"\n{'='*65}\n  {text}\n{'='*65}")
    else:
        print(f"\n  --- {text} ---")

def sorted_md5(path: Path) -> tuple:
    lines = []
    with open(path, encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                lines.append(json.dumps(json.loads(raw), sort_keys=True))
            except json.JSONDecodeError:
                lines.append(raw)
    lines.sort()
    return len(lines), hashlib.md5("\n".join(lines).encode()).hexdigest()

def run_cmd(cmd: list, check=True) -> subprocess.CompletedProcess:
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n{r.stderr.strip()}")
    return r

# -----------------------------------------------------------------------
# Step 1: Generate test data (50 MB — faster for multi-backend test)
# -----------------------------------------------------------------------

def generate_jsonl(path: Path, target_mb: int = 50) -> tuple:
    banner("Step 1: Generating test JSONL")
    services = ["api", "auth", "db", "cache", "worker"]
    levels   = ["INFO"] * 7 + ["WARN"] * 2 + ["ERROR"]
    rng      = random.Random(777)
    base_ts  = 1744200000
    ts       = base_ts
    count    = 0

    with open(path, "w") as f:
        while True:
            ts += rng.randint(0, 1)
            f.write(json.dumps({
                "ts": ts, "level": rng.choice(levels),
                "service": rng.choice(services),
                "message": f"All-backend test event {count}",
                "duration_ms": rng.randint(1, 500),
                "status": rng.choice([200, 200, 201, 400, 500]),
            }) + "\n")
            count += 1
            if count % 50_000 == 0:
                sz = path.stat().st_size / 1_048_576
                print(f"  {count:,} lines  |  {sz:.1f} MB", end="\r")
                if sz >= target_mb:
                    break

    ts_end = ts
    print(f"  {count:,} lines  |  {mb(path):.1f} MB  |  TS {base_ts} → {ts_end}  ✓")
    return count, base_ts, ts_end

# -----------------------------------------------------------------------
# Step 2: Compress
# -----------------------------------------------------------------------

def compress_all(src: Path, out_dir: Path) -> dict:
    banner("Step 2: Compressing to all formats")
    results = {}
    orig = mb(src)

    gz = out_dir / (src.stem + ".jsonl.gz")
    print("  gzip  ...", end=" ", flush=True)
    with open(src,"rb") as fi, gzip.open(gz,"wb",compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    print(f"{mb(gz):.1f} MB  ({mb(gz)/orig*100:.1f}%)  ✓")
    results["gz"] = gz

    bz = out_dir / (src.stem + ".jsonl.bz2")
    print("  bzip2 ...", end=" ", flush=True)
    with open(src,"rb") as fi, bz2.open(bz,"wb") as fo:
        shutil.copyfileobj(fi, fo)
    print(f"{mb(bz):.1f} MB  ({mb(bz)/orig*100:.1f}%)  ✓")
    results["bz2"] = bz

    zst = out_dir / (src.stem + ".jsonl.zst")
    print("  zstd  ...", end=" ", flush=True)
    import zstandard as zmod
    with open(src,"rb") as fi, open(zst,"wb") as fo:
        zmod.ZstdCompressor(level=3).copy_stream(fi, fo)
    print(f"{mb(zst):.1f} MB  ({mb(zst)/orig*100:.1f}%)  ✓")
    results["zst"] = zst

    lz = out_dir / (src.stem + ".jsonl.lz4")
    print("  lz4   ...", end=" ", flush=True)
    import lz4.frame
    with open(src,"rb") as fi, lz4.frame.open(lz,"wb") as fo:
        shutil.copyfileobj(fi, fo)
    print(f"{mb(lz):.1f} MB  ({mb(lz)/orig*100:.1f}%)  ✓")
    results["lz4"] = lz

    return results

# -----------------------------------------------------------------------
# Lossless check
# -----------------------------------------------------------------------

def check_lossless(pfc_path: Path, orig_count: int, orig_md5: str) -> bool:
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        run_cmd([PFC_BINARY, "decompress", "-q", str(pfc_path), tmp_path])
        count, digest = sorted_md5(Path(tmp_path))
        ok = (count == orig_count and digest == orig_md5)
        if ok:
            print(f"     Lossless ✅  {count:,} lines  MD5: {digest[:12]}...")
        else:
            print(f"     Lossless ❌  lines={count:,} (exp {orig_count:,})  md5={'ok' if digest==orig_md5 else 'MISMATCH'}")
        return ok
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

# -----------------------------------------------------------------------
# DuckDB random access check
# -----------------------------------------------------------------------

def extract_original_window(original: Path, ts_from: int, ts_to: int) -> tuple:
    """Extract lines from original JSONL for ts_from <= ts <= ts_to."""
    lines = []
    with open(original) as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                if ts_from <= obj.get("ts", 0) <= ts_to:
                    lines.append(json.dumps(obj, sort_keys=True))
            except Exception:
                pass
    lines.sort()
    return len(lines), hashlib.md5("\n".join(lines).encode()).hexdigest()

def check_random_access(pfc_path: Path, original: Path, ts_from: int, ts_to: int) -> bool:
    """
    Run DuckDB read_pfc_jsonl with ts_from/ts_to, compare to original.
    This is the key user-value check: random access without full decompression.
    read_pfc_jsonl does block-level coarse filtering; we do exact ts filtering in Python.
    """
    import csv as csv_mod
    out_file = f"/tmp/pfc_ra_{os.getpid()}.csv"
    # Use FORMAT CSV (not TEXT which doesn't exist) + write to temp file
    # Block-level query returns all lines in matching blocks; Python filters exactly
    duckdb_sql = f"""
LOAD pfc;
COPY (
  SELECT line FROM read_pfc_jsonl('{pfc_path}', ts_from={ts_from}, ts_to={ts_to})
) TO '{out_file}' (FORMAT CSV, HEADER false);
"""
    try:
        r = subprocess.run(
            [DUCKDB_BIN, "-c", duckdb_sql],
            capture_output=True, text=True
        )
        if r.returncode != 0:
            print(f"     DuckDB error: {r.stderr.strip()[:200]}")
            return False

        # Parse CSV output (DuckDB quotes JSON strings) + filter by ts exactly
        pfc_lines = []
        with open(out_file, newline="", encoding="utf-8") as f:
            for row in csv_mod.reader(f):
                if not row:
                    continue
                raw = row[0].strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                    ts_val = obj.get("ts", 0)
                    if ts_from <= ts_val <= ts_to:
                        pfc_lines.append(json.dumps(obj, sort_keys=True))
                except Exception:
                    pass
        pfc_lines.sort()

        orig_count, orig_md5 = extract_original_window(original, ts_from, ts_to)
        pfc_md5 = hashlib.md5("\n".join(pfc_lines).encode()).hexdigest()

        if len(pfc_lines) == orig_count and pfc_md5 == orig_md5:
            print(f"     Random access ✅  {orig_count:,} lines in window  MD5 match")
            return True
        else:
            print(f"     Random access ❌  pfc={len(pfc_lines):,} orig={orig_count:,}  md5={'ok' if pfc_md5==orig_md5 else 'MISMATCH'}")
            return False
    finally:
        if os.path.exists(out_file):
            os.unlink(out_file)

# -----------------------------------------------------------------------
# Local backend test
# -----------------------------------------------------------------------

def test_local(compressed: dict, original: Path, orig_count: int, orig_md5: str,
               ts_from: int, ts_to: int, work_dir: Path) -> dict:
    banner("Backend: LOCAL FILESYSTEM", level=1)
    results = {}
    for fmt, src in compressed.items():
        print(f"\n  [{fmt}]")
        out = work_dir / f"local_{fmt}.pfc"
        try:
            run_cmd(["python3", MIGRATE_PY, "convert", str(src), str(out)])
            lossless = check_lossless(out, orig_count, orig_md5)
            ra       = check_random_access(out, original, ts_from, ts_to)
            results[fmt] = lossless and ra
        except Exception as e:
            print(f"     ERROR: {e}")
            results[fmt] = False
    return results

# -----------------------------------------------------------------------
# S3 / MinIO backend test
# -----------------------------------------------------------------------

def test_s3(compressed: dict, original: Path, orig_count: int, orig_md5: str,
            ts_from: int, ts_to: int, work_dir: Path) -> dict:
    banner("Backend: S3 (MinIO)", level=1)
    import boto3
    s3 = boto3.client("s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
        region_name=S3_REGION,
    )
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
    except Exception:
        pass

    results = {}
    for fmt, src in compressed.items():
        print(f"\n  [{fmt}]")
        key     = f"input/{src.name}"
        out_key = f"output/{src.stem.replace('.jsonl','')}.pfc"
        out_pfc = work_dir / f"s3_{fmt}.pfc"
        out_bidx= work_dir / f"s3_{fmt}.pfc.bidx"
        try:
            # Upload
            s3.upload_file(str(src), S3_BUCKET, key)

            # Convert
            run_cmd([
                "python3", MIGRATE_PY, "s3",
                "--bucket", S3_BUCKET, "--key", key,
                "--out-bucket", S3_BUCKET, "--out-prefix", "output",
                "--endpoint-url", S3_ENDPOINT,
                "--access-key", S3_ACCESS, "--secret-key", S3_SECRET,
                "--region", S3_REGION, "--format", fmt,
            ])

            # Find and download .pfc
            resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="output/")
            pfc_keys = [o["Key"] for o in resp.get("Contents", [])
                        if o["Key"].endswith(".pfc") and not o["Key"].endswith(".bidx")]
            if not pfc_keys:
                raise RuntimeError("No .pfc found in output/")
            s3.download_file(S3_BUCKET, pfc_keys[-1], str(out_pfc))
            try:
                s3.download_file(S3_BUCKET, pfc_keys[-1] + ".bidx", str(out_bidx))
            except Exception:
                pass

            # Verify
            lossless = check_lossless(out_pfc, orig_count, orig_md5)
            ra       = check_random_access(out_pfc, original, ts_from, ts_to)
            results[fmt] = lossless and ra

            # Cleanup S3
            for o in resp.get("Contents", []):
                s3.delete_object(Bucket=S3_BUCKET, Key=o["Key"])
            s3.delete_object(Bucket=S3_BUCKET, Key=key)

        except Exception as e:
            print(f"     ERROR: {e}")
            results[fmt] = False
    return results

# -----------------------------------------------------------------------
# Azure / Azurite backend test
# -----------------------------------------------------------------------

def test_azure(compressed: dict, original: Path, orig_count: int, orig_md5: str,
               ts_from: int, ts_to: int, work_dir: Path) -> dict:
    banner("Backend: AZURE BLOB (Azurite)", level=1)
    from azure.storage.blob import BlobServiceClient
    client = BlobServiceClient.from_connection_string(AZURE_CONN)

    try:
        client.create_container(AZURE_CONTAINER)
    except Exception:
        pass

    results = {}
    for fmt, src in compressed.items():
        print(f"\n  [{fmt}]")
        blob_name = f"input/{src.name}"
        out_pfc   = work_dir / f"azure_{fmt}.pfc"
        out_bidx  = work_dir / f"azure_{fmt}.pfc.bidx"
        try:
            # Upload
            bc = client.get_blob_client(AZURE_CONTAINER, blob_name)
            with open(src, "rb") as f:
                bc.upload_blob(f, overwrite=True)

            # Convert
            run_cmd([
                "python3", MIGRATE_PY, "azure",
                "--container", AZURE_CONTAINER,
                "--blob", blob_name,
                "--out-container", AZURE_CONTAINER,
                "--out-prefix", "output",
                "--connection-string", AZURE_CONN,
                "--format", fmt,
            ])

            # Find and download .pfc
            cc = client.get_container_client(AZURE_CONTAINER)
            pfc_blobs = [b.name for b in cc.list_blobs(name_starts_with="output/")
                         if b.name.endswith(".pfc") and not b.name.endswith(".bidx")]
            if not pfc_blobs:
                raise RuntimeError("No .pfc blob found in output/")

            with open(out_pfc, "wb") as f:
                f.write(client.get_blob_client(AZURE_CONTAINER, pfc_blobs[-1]).download_blob().readall())
            try:
                with open(out_bidx, "wb") as f:
                    f.write(client.get_blob_client(AZURE_CONTAINER, pfc_blobs[-1]+".bidx").download_blob().readall())
            except Exception:
                pass

            lossless = check_lossless(out_pfc, orig_count, orig_md5)
            ra       = check_random_access(out_pfc, original, ts_from, ts_to)
            results[fmt] = lossless and ra

            # Cleanup
            for b in cc.list_blobs():
                client.get_blob_client(AZURE_CONTAINER, b.name).delete_blob()

        except Exception as e:
            print(f"     ERROR: {e}")
            results[fmt] = False
    return results

# -----------------------------------------------------------------------
# GCS / fake-gcs backend test
# -----------------------------------------------------------------------

def test_gcs(compressed: dict, original: Path, orig_count: int, orig_md5: str,
             ts_from: int, ts_to: int, work_dir: Path) -> dict:
    banner("Backend: GCS (fake-gcs-server)", level=1)
    from google.cloud import storage as gcs_mod
    from google.auth.credentials import AnonymousCredentials

    client = gcs_mod.Client(
        credentials=AnonymousCredentials(),
        project="test-project",
        client_options={"api_endpoint": GCS_ENDPOINT},
    )
    client._http.verify = False

    try:
        client.create_bucket(GCS_BUCKET)
    except Exception:
        pass

    results = {}
    for fmt, src in compressed.items():
        print(f"\n  [{fmt}]")
        blob_name = f"input/{src.name}"
        out_pfc   = work_dir / f"gcs_{fmt}.pfc"
        out_bidx  = work_dir / f"gcs_{fmt}.pfc.bidx"
        try:
            # Upload
            bucket = client.bucket(GCS_BUCKET)
            bucket.blob(blob_name).upload_from_filename(str(src))

            # Convert
            run_cmd([
                "python3", MIGRATE_PY, "gcs",
                "--bucket", GCS_BUCKET,
                "--blob", blob_name,
                "--out-bucket", GCS_BUCKET,
                "--out-prefix", "output",
                "--endpoint-url", GCS_ENDPOINT,
                "--format", fmt,
            ])

            # Find and download .pfc
            blobs = [b.name for b in client.list_blobs(GCS_BUCKET, prefix="output/")
                     if b.name.endswith(".pfc") and not b.name.endswith(".bidx")]
            if not blobs:
                raise RuntimeError("No .pfc found in output/")

            bucket.blob(blobs[-1]).download_to_filename(str(out_pfc))
            try:
                bucket.blob(blobs[-1]+".bidx").download_to_filename(str(out_bidx))
            except Exception:
                pass

            lossless = check_lossless(out_pfc, orig_count, orig_md5)
            ra       = check_random_access(out_pfc, original, ts_from, ts_to)
            results[fmt] = lossless and ra

            # Cleanup
            for b in client.list_blobs(GCS_BUCKET):
                b.delete()

        except Exception as e:
            print(f"     ERROR: {e}")
            results[fmt] = False
    return results

# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    TEST_DIR.mkdir(parents=True, exist_ok=True)
    work_dir = TEST_DIR / "work"
    work_dir.mkdir(exist_ok=True)
    original    = TEST_DIR / "original.jsonl"
    t0 = time.time()

    # Generate
    total_lines, ts_start, ts_end = generate_jsonl(original, target_mb=50)

    # Compress
    compressed = compress_all(original, TEST_DIR)

    # Pre-compute original MD5 (full)
    print("\n  Computing original MD5 ...", end=" ", flush=True)
    orig_count, orig_md5 = sorted_md5(original)
    print(f"{orig_count:,} lines  |  MD5: {orig_md5[:16]}...")

    # Choose time window for random access test (middle 10%)
    window     = (ts_end - ts_start) // 10
    ts_from_ra = ts_start + window * 4
    ts_to_ra   = ts_start + window * 5
    print(f"  Random access window: ts {ts_from_ra} → {ts_to_ra}")

    # Run tests per backend
    all_results = {}
    all_results["local"] = test_local(compressed, original, orig_count, orig_md5,
                                       ts_from_ra, ts_to_ra, work_dir)
    all_results["s3"]    = test_s3(compressed, original, orig_count, orig_md5,
                                    ts_from_ra, ts_to_ra, work_dir)
    all_results["azure"] = test_azure(compressed, original, orig_count, orig_md5,
                                       ts_from_ra, ts_to_ra, work_dir)
    all_results["gcs"]   = test_gcs(compressed, original, orig_count, orig_md5,
                                     ts_from_ra, ts_to_ra, work_dir)

    # Final report
    banner("FINAL REPORT — All Backends × All Formats")
    print(f"  {'Backend':<8}  {'gz':<8}  {'bz2':<8}  {'zst':<8}  {'lz4':<8}  {'Overall'}")
    print(f"  {'-'*60}")

    all_pass = True
    for backend, results in all_results.items():
        row = []
        backend_pass = True
        for fmt in FORMATS:
            passed = results.get(fmt, False)
            row.append("✅" if passed else "❌")
            if not passed:
                backend_pass = False
                all_pass = False
        overall = "✅ ALL PASS" if backend_pass else "❌ FAIL"
        print(f"  {backend:<8}  {'  '.join(row)}  {overall}")

    elapsed = time.time() - t0
    print(f"\n  Total time : {elapsed:.0f}s")
    print(f"\n  {'🏆 ALL BACKENDS × ALL FORMATS — LOSSLESS + RANDOM ACCESS VERIFIED' if all_pass else '❌ SOME TESTS FAILED'}\n")

    shutil.rmtree(TEST_DIR, ignore_errors=True)
    print("  Test files cleaned up.")
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
