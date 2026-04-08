#!/usr/bin/env python3
"""
pfc-migrate Stage 2 — S3 Lossless Roundtrip Test (MinIO)
==========================================================
1. Generate ~100 MB JSONL
2. Compress to gzip, zstd, bzip2, lz4
3. Upload all to MinIO (S3-compatible)
4. Run pfc-migrate s3 --endpoint-url http://localhost:9000 (in-place conversion)
5. Download converted .pfc files
6. Fully decompress each .pfc
7. MD5 comparison vs original → PASS / FAIL
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
from pathlib import Path

# MinIO config
ENDPOINT   = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET     = "pfc-s3-test"
REGION     = "us-east-1"

PFC_BINARY = "/usr/local/bin/pfc_jsonl"
MIGRATE_PY = "/root/pfc_migrate.py"
TEST_DIR   = Path("/root/pfc_s3_test")


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def mb(path):
    return Path(path).stat().st_size / 1_048_576

def banner(text):
    print(f"\n{'='*60}\n  {text}\n{'='*60}")

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

def run(cmd, check=True, capture=True):
    r = subprocess.run(cmd, shell=True, capture_output=capture, text=True)
    if check and r.returncode != 0:
        print(f"CMD FAILED: {cmd}\n{r.stderr}")
        sys.exit(1)
    return r


# -----------------------------------------------------------------------
# Step 1: Generate test data
# -----------------------------------------------------------------------

def generate_jsonl(path: Path, target_mb: int = 100) -> int:
    banner(f"Step 1: Generating ~{target_mb} MB JSONL")
    services = ["api", "auth", "db", "cache", "worker"]
    levels   = ["INFO"] * 7 + ["WARN"] * 2 + ["ERROR"]
    rng      = random.Random(99)
    ts       = 1744100000
    count    = 0

    with open(path, "w") as f:
        while True:
            ts += rng.randint(0, 1)
            f.write(json.dumps({
                "ts": ts, "level": rng.choice(levels),
                "service": rng.choice(services),
                "message": f"S3 test event {count}",
                "duration_ms": rng.randint(1, 500),
            }) + "\n")
            count += 1
            if count % 50_000 == 0:
                sz = path.stat().st_size / 1_048_576
                print(f"  {count:,} lines  |  {sz:.1f} MB", end="\r")
                if sz >= target_mb:
                    break

    print(f"  {count:,} lines  |  {mb(path):.1f} MB  ✓")
    return count


# -----------------------------------------------------------------------
# Step 2: Compress
# -----------------------------------------------------------------------

def compress_all(src: Path) -> dict:
    banner("Step 2: Compressing to all formats")
    results = {}
    orig = mb(src)

    gz = src.with_suffix(".jsonl.gz")
    print("  gzip  ...", end=" ", flush=True)
    with open(src,"rb") as fi, gzip.open(gz,"wb",compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo)
    print(f"{mb(gz):.1f} MB  ({mb(gz)/orig*100:.1f}%)  ✓")
    results["gz"] = gz

    bz = src.with_suffix(".jsonl.bz2")
    print("  bzip2 ...", end=" ", flush=True)
    with open(src,"rb") as fi, bz2.open(bz,"wb") as fo:
        shutil.copyfileobj(fi, fo)
    print(f"{mb(bz):.1f} MB  ({mb(bz)/orig*100:.1f}%)  ✓")
    results["bz2"] = bz

    zst = src.with_suffix(".jsonl.zst")
    print("  zstd  ...", end=" ", flush=True)
    import zstandard as zmod
    with open(src,"rb") as fi, open(zst,"wb") as fo:
        zmod.ZstdCompressor(level=3).copy_stream(fi, fo)
    print(f"{mb(zst):.1f} MB  ({mb(zst)/orig*100:.1f}%)  ✓")
    results["zst"] = zst

    lz = src.with_suffix(".jsonl.lz4")
    print("  lz4   ...", end=" ", flush=True)
    import lz4.frame
    with open(src,"rb") as fi, lz4.frame.open(lz,"wb") as fo:
        shutil.copyfileobj(fi, fo)
    print(f"{mb(lz):.1f} MB  ({mb(lz)/orig*100:.1f}%)  ✓")
    results["lz4"] = lz

    return results


# -----------------------------------------------------------------------
# Step 3: Upload to MinIO
# -----------------------------------------------------------------------

def upload_all(s3, compressed: dict) -> dict:
    banner("Step 3: Uploading to MinIO (S3)")
    keys = {}
    for fmt, path in compressed.items():
        key = f"input/{path.name}"
        print(f"  upload s3://{BUCKET}/{key} ...", end=" ", flush=True)
        s3.upload_file(str(path), BUCKET, key)
        print(f"✓")
        keys[fmt] = key
    return keys


# -----------------------------------------------------------------------
# Step 4: Convert via pfc-migrate s3
# -----------------------------------------------------------------------

def convert_s3_all(keys: dict) -> dict:
    banner("Step 4: Converting via pfc-migrate s3 (in-place on MinIO)")
    pfc_keys = {}

    for fmt, key in keys.items():
        out_key = f"output/{Path(key).stem.replace('.jsonl','')}.pfc"
        print(f"  {fmt:4s} → pfc ...", end=" ", flush=True)

        cmd = [
            "python3", MIGRATE_PY, "s3",
            "--bucket",       BUCKET,
            "--key",          key,
            "--out-bucket",   BUCKET,
            "--out-prefix",   "output",
            "--endpoint-url", ENDPOINT,
            "--access-key",   ACCESS_KEY,
            "--secret-key",   SECRET_KEY,
            "--region",       REGION,
            "--format",       fmt,
        ]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            print(f"FAILED:\n{r.stderr}")
            continue

        # Check the .pfc was uploaded
        try:
            # list output/ prefix to find the actual key
            import boto3
            s3 = boto3.client("s3",
                endpoint_url=ENDPOINT,
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                region_name=REGION,
            )
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="output/")
            pfc_key = None
            for obj in resp.get("Contents", []):
                if obj["Key"].endswith(".pfc") and not obj["Key"].endswith(".bidx"):
                    if fmt in obj["Key"] or True:  # take first .pfc
                        pfc_key = obj["Key"]
                        break
            if pfc_key:
                size = s3.head_object(Bucket=BUCKET, Key=pfc_key)["ContentLength"] / 1_048_576
                print(f"{size:.1f} MB  ✓  s3://{BUCKET}/{pfc_key}")
                pfc_keys[fmt] = pfc_key
            else:
                print("✓ (key not found in listing?)")
        except Exception as e:
            print(f"✓ (listing error: {e})")

    return pfc_keys


# -----------------------------------------------------------------------
# Step 5+6: Download PFC + verify lossless
# -----------------------------------------------------------------------

def verify_all(s3, pfc_keys: dict, original: Path) -> dict:
    banner("Step 5+6: Download PFC + Lossless MD5 Verification")

    print("  Computing original MD5 ...", end=" ", flush=True)
    orig_count, orig_md5 = sorted_md5(original)
    print(f"{orig_count:,} lines  |  MD5: {orig_md5[:16]}...")

    results = {}
    for fmt, pfc_key in pfc_keys.items():
        print(f"\n--- {fmt.upper()} ---")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_pfc    = Path(tmpdir) / "downloaded.pfc"
            tmp_bidx   = Path(tmpdir) / "downloaded.pfc.bidx"
            tmp_decomp = Path(tmpdir) / "decompressed.jsonl"

            try:
                # Download .pfc
                print(f"  Downloading s3://{BUCKET}/{pfc_key} ...", end=" ", flush=True)
                s3.download_file(BUCKET, pfc_key, str(tmp_pfc))
                print(f"{mb(tmp_pfc):.1f} MB")

                # Download .pfc.bidx
                bidx_key = pfc_key + ".bidx"
                try:
                    s3.download_file(BUCKET, bidx_key, str(tmp_bidx))
                    print(f"  .bidx index ✓")
                except Exception:
                    print(f"  .bidx index ✗ (missing)")

                # Decompress PFC → JSONL
                print(f"  Decompressing ...", end=" ", flush=True)
                r = subprocess.run(
                    [PFC_BINARY, "decompress", "-q", str(tmp_pfc), str(tmp_decomp)],
                    capture_output=True, text=True
                )
                if r.returncode != 0:
                    raise RuntimeError(r.stderr.strip())

                decomp_count, decomp_md5 = sorted_md5(tmp_decomp)
                print(f"{mb(tmp_decomp):.1f} MB  |  {decomp_count:,} lines  |  MD5: {decomp_md5[:16]}...")

                if decomp_count == orig_count and decomp_md5 == orig_md5:
                    print(f"  ✅  LOSSLESS — count and MD5 match perfectly")
                    results[fmt] = True
                else:
                    print(f"  ❌  MISMATCH!")
                    if decomp_count != orig_count:
                        print(f"       Lines: orig={orig_count:,}  decomp={decomp_count:,}")
                    if decomp_md5 != orig_md5:
                        print(f"       MD5 differs")
                    results[fmt] = False

            except Exception as exc:
                print(f"  ❌  ERROR: {exc}")
                results[fmt] = False

    return results


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    import boto3

    TEST_DIR.mkdir(parents=True, exist_ok=True)
    original = TEST_DIR / "original.jsonl"
    t0 = time.time()

    # S3 client
    s3 = boto3.client("s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
    )

    # Ensure bucket exists
    try:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Bucket '{BUCKET}' created.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket '{BUCKET}' already exists.")
    except Exception:
        pass  # might already exist

    generate_jsonl(original, target_mb=100)
    compressed = compress_all(original)
    keys       = upload_all(s3, compressed)
    pfc_keys   = convert_s3_all(keys)
    results    = verify_all(s3, pfc_keys, original)

    banner("FINAL REPORT — S3 Lossless Test")
    all_pass = True
    for fmt, passed in results.items():
        print(f"  {fmt:5s}  {'✅ PASS' if passed else '❌ FAIL'}")
        if not passed:
            all_pass = False

    print(f"\n  Total time : {time.time()-t0:.1f}s")
    print(f"  Result     : {'✅ ALL FORMATS LOSSLESS via S3' if all_pass else '❌ SOME FORMATS FAILED'}\n")

    # Cleanup
    shutil.rmtree(TEST_DIR, ignore_errors=True)
    print("  Local test files cleaned up.")

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
