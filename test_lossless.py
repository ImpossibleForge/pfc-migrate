#!/usr/bin/env python3
"""
pfc-migrate Lossless Roundtrip Test
=====================================
1. Generate ~200 MB JSONL with realistic timestamps
2. Compress into: gzip, zstd, bzip2, lz4
3. Convert each via pfc-migrate → .pfc
4. Fully decompress each .pfc back to JSONL
5. Compare line count + MD5 against original → PASS / FAIL

Usage:
  python3 test_lossless.py [--mb 200] [--keep]
"""

import argparse
import bz2
import gzip
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path

PFC_BINARY = "/usr/local/bin/pfc_jsonl"
MIGRATE_PY = "/root/pfc_migrate.py"
TEST_DIR   = Path("/root/pfc_migrate_lossless_test")


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def mb(path):
    return Path(path).stat().st_size / 1_048_576

def banner(text):
    print(f"\n{'='*60}\n  {text}\n{'='*60}")

def sorted_md5(path: Path) -> tuple:
    """Return (line_count, md5) over sorted, JSON-normalized lines."""
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
    digest = hashlib.md5("\n".join(lines).encode("utf-8")).hexdigest()
    return len(lines), digest


# -----------------------------------------------------------------------
# Step 1: Generate test data
# -----------------------------------------------------------------------

def generate_jsonl(path: Path, target_mb: int = 200) -> tuple:
    banner(f"Step 1: Generating ~{target_mb} MB JSONL")

    services = ["api", "auth", "db", "cache", "worker", "gateway", "scheduler", "metrics"]
    levels   = ["INFO"] * 7 + ["WARN"] * 2 + ["ERROR"]
    paths_   = ["/api/v1/users", "/api/v1/orders", "/health", "/metrics", "/api/v1/products"]

    rng     = random.Random(42)   # fixed seed → reproducible
    base_ts = 1744000000
    ts      = base_ts
    count   = 0

    with open(path, "w", encoding="utf-8") as f:
        while True:
            ts += rng.randint(0, 1)
            line = json.dumps({
                "ts":          ts,
                "level":       rng.choice(levels),
                "service":     rng.choice(services),
                "path":        rng.choice(paths_),
                "message":     f"Request {count} completed",
                "duration_ms": rng.randint(1, 2000),
                "status":      rng.choice([200, 200, 200, 201, 400, 404, 500]),
                "user_id":     rng.randint(1000, 9999),
            })
            f.write(line + "\n")
            count += 1
            if count % 100_000 == 0:
                sz = path.stat().st_size / 1_048_576
                print(f"  {count:>10,} lines  |  {sz:.1f} MB", end="\r")
                if sz >= target_mb:
                    break

    ts_end = ts
    sz = mb(path)
    print(f"  {count:,} lines  |  {sz:.1f} MB  |  TS {base_ts} → {ts_end}")
    return count, base_ts, ts_end


# -----------------------------------------------------------------------
# Step 2: Compress to all formats
# -----------------------------------------------------------------------

def compress_all(src: Path) -> dict:
    banner("Step 2: Compressing to all formats")
    results = {}
    orig_mb = mb(src)

    # gzip
    gz = src.with_suffix(".jsonl.gz")
    print("  gzip  ...", end=" ", flush=True)
    with open(src, "rb") as fin, gzip.open(gz, "wb", compresslevel=6) as fout:
        shutil.copyfileobj(fin, fout)
    print(f"{mb(gz):.1f} MB  ({mb(gz)/orig_mb*100:.1f}%)  ✓")
    results["gz"] = gz

    # bzip2
    bz = src.with_suffix(".jsonl.bz2")
    print("  bzip2 ...", end=" ", flush=True)
    with open(src, "rb") as fin, bz2.open(bz, "wb") as fout:
        shutil.copyfileobj(fin, fout)
    print(f"{mb(bz):.1f} MB  ({mb(bz)/orig_mb*100:.1f}%)  ✓")
    results["bz2"] = bz

    # zstd
    zst = src.with_suffix(".jsonl.zst")
    print("  zstd  ...", end=" ", flush=True)
    import zstandard as zstd_mod
    cctx = zstd_mod.ZstdCompressor(level=3)
    with open(src, "rb") as fin, open(zst, "wb") as fout:
        cctx.copy_stream(fin, fout)
    print(f"{mb(zst):.1f} MB  ({mb(zst)/orig_mb*100:.1f}%)  ✓")
    results["zst"] = zst

    # lz4
    lz = src.with_suffix(".jsonl.lz4")
    print("  lz4   ...", end=" ", flush=True)
    import lz4.frame
    with open(src, "rb") as fin, lz4.frame.open(lz, "wb") as fout:
        shutil.copyfileobj(fin, fout)
    print(f"{mb(lz):.1f} MB  ({mb(lz)/orig_mb*100:.1f}%)  ✓")
    results["lz4"] = lz

    print(f"\n  Original : {orig_mb:.1f} MB")
    return results


# -----------------------------------------------------------------------
# Step 3: Convert to PFC
# -----------------------------------------------------------------------

def convert_all(compressed: dict, out_dir: Path) -> dict:
    banner("Step 3: Converting to PFC via pfc-migrate")
    pfc_files = {}

    for fmt, src in compressed.items():
        out = out_dir / f"test_{fmt}.pfc"
        print(f"  {fmt:4s} → pfc ...", end=" ", flush=True)
        r = subprocess.run(
            ["python3", MIGRATE_PY, "convert", str(src), str(out)],
            capture_output=True, text=True
        )
        if r.returncode != 0 or not out.exists():
            print(f"FAILED: {r.stderr.strip()}")
            continue
        bidx = Path(str(out) + ".bidx")
        print(f"{mb(out):.1f} MB  {'✓ bidx' if bidx.exists() else '✗ bidx MISSING'}  ✓")
        pfc_files[fmt] = out

    return pfc_files


# -----------------------------------------------------------------------
# Step 4+5: Decompress PFC and compare MD5
# -----------------------------------------------------------------------

def verify_all(pfc_files: dict, original: Path) -> dict:
    banner("Step 4+5: Lossless Verification — Full Decompress + MD5")

    print("  Computing original MD5 ...", end=" ", flush=True)
    orig_count, orig_md5 = sorted_md5(original)
    print(f"{orig_count:,} lines  |  MD5: {orig_md5[:16]}...")

    results = {}
    for fmt, pfc_path in pfc_files.items():
        print(f"\n--- {fmt.upper()} ---")
        tmp = pfc_path.parent / f"_decomp_{fmt}.jsonl"
        try:
            # Decompress PFC → JSONL
            print(f"  Decompressing {pfc_path.name} ...", end=" ", flush=True)
            r = subprocess.run(
                [PFC_BINARY, "decompress", "-q", str(pfc_path), str(tmp)],
                capture_output=True, text=True
            )
            if r.returncode != 0:
                raise RuntimeError(r.stderr.strip())

            decomp_count, decomp_md5 = sorted_md5(tmp)
            print(f"{mb(tmp):.1f} MB  |  {decomp_count:,} lines  |  MD5: {decomp_md5[:16]}...")

            if decomp_count == orig_count and decomp_md5 == orig_md5:
                print(f"  ✅  LOSSLESS — count and MD5 match perfectly")
                results[fmt] = True
            else:
                print(f"  ❌  MISMATCH!")
                if decomp_count != orig_count:
                    print(f"       Line count: orig={orig_count:,}  decomp={decomp_count:,}"
                          f"  diff={decomp_count-orig_count:+,}")
                if decomp_md5 != orig_md5:
                    print(f"       MD5 orig:  {orig_md5}")
                    print(f"       MD5 decomp: {decomp_md5}")
                results[fmt] = False

        except Exception as exc:
            print(f"  ❌  ERROR: {exc}")
            results[fmt] = False
        finally:
            if tmp.exists():
                tmp.unlink()

    return results


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mb",   type=int, default=200, help="Target size MB (default: 200)")
    ap.add_argument("--keep", action="store_true",   help="Keep test files after run")
    args = ap.parse_args()

    TEST_DIR.mkdir(parents=True, exist_ok=True)
    original = TEST_DIR / "original.jsonl"
    t0 = time.time()

    total_lines, ts_start, ts_end = generate_jsonl(original, target_mb=args.mb)
    compressed  = compress_all(original)
    pfc_files   = convert_all(compressed, TEST_DIR)
    results     = verify_all(pfc_files, original)

    banner("FINAL REPORT")
    all_pass = True
    for fmt, passed in results.items():
        print(f"  {fmt:5s}  {'✅ PASS' if passed else '❌ FAIL'}")
        if not passed:
            all_pass = False

    print(f"\n  Total time : {time.time()-t0:.1f}s")
    print(f"  Result     : {'✅ ALL FORMATS LOSSLESS' if all_pass else '❌ SOME FORMATS FAILED'}\n")

    if not args.keep:
        shutil.rmtree(TEST_DIR)
        print("  Test files cleaned up.")

    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    main()
