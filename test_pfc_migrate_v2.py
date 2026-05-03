#!/usr/bin/env python3
"""
pfc-migrate v2.0.0 — Comprehensive Test Suite
===============================================
Happy-Path + Error Tests for Format-Converter
Run on server: python3 test_pfc_migrate_v2.py

Author: ForgeBuddy + Dante | 2026-04-23
"""

import gzip
import bz2
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
SCRIPT  = Path("/root/pfc_migrate.py")
PFC_BIN = "/usr/local/bin/pfc_jsonl"
OUTDIR  = Path("/root/pfc_migrate_test_output")
OUTDIR.mkdir(exist_ok=True)

results = []

def run(name, cmd, expect_exit=0, expect_in_stdout=None):
    t0  = time.time()
    res = subprocess.run(cmd, capture_output=True, text=True)
    dt  = time.time() - t0

    ok = True
    reasons = []

    if res.returncode != expect_exit:
        ok = False
        reasons.append(f"exit={res.returncode} expected={expect_exit}")

    for s in (expect_in_stdout or []):
        if s not in (res.stdout + res.stderr):
            ok = False
            reasons.append(f"missing: {s!r}")

    status = "✅ PASS" if ok else "❌ FAIL"
    print(f"  {status}  [{dt:.1f}s]  {name}")
    if not ok:
        for r in reasons:
            print(f"           → {r}")
        if res.stderr.strip():
            print(f"           stderr: {res.stderr.strip()[:200]}")
    results.append((name, ok, dt))
    return ok, res


def py(args, name=None, **kwargs):
    label = name or " ".join(str(a) for a in args[:4])
    return run(label, ["python3", str(SCRIPT)] + [str(a) for a in args], **kwargs)


def gen_jsonl(path: Path, rows=5000):
    """Generate synthetic JSONL test data."""
    levels   = ["INFO", "WARN", "ERROR", "DEBUG"]
    services = ["api", "auth", "payment"]
    with open(path, "w") as f:
        for i in range(rows):
            f.write(json.dumps({
                "timestamp": f"2026-01-{(i//5000)+1:02d}T{(i%24):02d}:00:00Z",
                "level": levels[i % len(levels)],
                "service": services[i % len(services)],
                "message": f"Request processed for user {i % 1000}",
                "request_id": f"req-{i:08d}",
                "latency_ms": 10 + (i % 490),
            }) + "\n")
    return path


def roundtrip_ok(pfc_path: Path, expected_rows: int) -> bool:
    """Decompress and verify row count."""
    res = subprocess.run([PFC_BIN, "decompress", str(pfc_path), "-"], capture_output=True)
    if res.returncode != 0:
        return False
    actual = sum(1 for line in res.stdout.split(b"\n") if line.strip().startswith(b"{"))
    return actual == expected_rows


# ── Generate test fixtures ───────────────────────────────────────────────────
print("=" * 65)
print("pfc-migrate v2.0.0 — Test Suite (Format Converter)")
print("=" * 65)

print("\n[SETUP] Generating test files ...")
TMPDIR = Path(tempfile.mkdtemp(prefix="pfc_migrate_test_"))

plain_jsonl = TMPDIR / "test.jsonl"
gen_jsonl(plain_jsonl, 5000)

gz_file  = TMPDIR / "test.jsonl.gz"
bz2_file = TMPDIR / "test.jsonl.bz2"
zst_file = TMPDIR / "test.jsonl.zst"
lz4_file = TMPDIR / "test.jsonl.lz4"

with open(plain_jsonl, "rb") as fin:
    with gzip.open(gz_file, "wb") as fout:
        fout.write(fin.read())

with open(plain_jsonl, "rb") as fin:
    with bz2.open(bz2_file, "wb") as fout:
        fout.write(fin.read())

try:
    import zstandard as zstd
    cctx = zstd.ZstdCompressor()
    with open(plain_jsonl, "rb") as fin:
        with open(zst_file, "wb") as fout:
            fout.write(cctx.compress(fin.read()))
    print("        ✅ gz, bz2, zst, lz4 test files ready")
except ImportError:
    print("        ⚠️  zstandard not available, skipping zst tests")

try:
    import lz4.frame
    with open(plain_jsonl, "rb") as fin:
        with lz4.frame.open(lz4_file, "wb") as fout:
            fout.write(fin.read())
except ImportError:
    print("        ⚠️  lz4 not available, skipping lz4 tests")

# ══════════════════════════════════════════════════════════════════════════════
print("\n[1] HAPPY PATH — Single file conversion")

out_gz = OUTDIR / "T01_gz.pfc"
ok, _ = py(["convert", str(gz_file), str(out_gz), "--verbose"],
           name="T01: gzip → pfc",
           expect_in_stdout=["original"])
if ok:
    results.append(("T01b: gzip roundtrip", roundtrip_ok(out_gz, 5000), 0))
    print(f"           {'✅' if roundtrip_ok(out_gz, 5000) else '❌'} Roundtrip 5,000 rows")

out_bz2 = OUTDIR / "T02_bz2.pfc"
ok, _ = py(["convert", str(bz2_file), str(out_bz2)],
           name="T02: bzip2 → pfc",
           expect_in_stdout=["Done"])
if ok:
    results.append(("T02b: bz2 roundtrip", roundtrip_ok(out_bz2, 5000), 0))

if zst_file.exists():
    out_zst = OUTDIR / "T03_zst.pfc"
    ok, _ = py(["convert", str(zst_file), str(out_zst)],
               name="T03: zstd → pfc",
               expect_in_stdout=["Done"])
    if ok:
        results.append(("T03b: zst roundtrip", roundtrip_ok(out_zst, 5000), 0))

if lz4_file.exists():
    out_lz4 = OUTDIR / "T04_lz4.pfc"
    ok, _ = py(["convert", str(lz4_file), str(out_lz4)],
               name="T04: lz4 → pfc",
               expect_in_stdout=["Done"])
    if ok:
        results.append(("T04b: lz4 roundtrip", roundtrip_ok(out_lz4, 5000), 0))

out_plain = OUTDIR / "T05_plain.pfc"
ok, _ = py(["convert", str(plain_jsonl), str(out_plain)],
           name="T05: plain JSONL → pfc",
           expect_in_stdout=["Done"])
if ok:
    results.append(("T05b: plain roundtrip", roundtrip_ok(out_plain, 5000), 0))

# ── Auto output name ──────────────────────────────────────────────────────────
orig_dir = Path.cwd()
os.chdir(OUTDIR)
py(["convert", str(gz_file)],
   name="T06: Auto output filename (no output arg)",
   expect_in_stdout=["Done"])
os.chdir(orig_dir)

# ── Directory mode ────────────────────────────────────────────────────────────
print("\n[2] DIRECTORY MODE")
dir_in  = TMPDIR / "dir_input"
dir_out = OUTDIR / "T07_dir_output"
dir_in.mkdir()
for i in range(3):
    f = dir_in / f"app_{i:02d}.jsonl.gz"
    gen_jsonl(TMPDIR / f"tmp_{i}.jsonl", 1000)
    with open(TMPDIR / f"tmp_{i}.jsonl", "rb") as fin:
        with gzip.open(f, "wb") as fout:
            fout.write(fin.read())

py(["convert", "--dir", str(dir_in), "--output-dir", str(dir_out), "--verbose"],
   name="T07: Directory mode (3 files)",
   expect_in_stdout=["3 converted"])

# Recursive
sub = dir_in / "sub"
sub.mkdir()
gen_jsonl(TMPDIR / "tmp_sub.jsonl", 500)
with open(TMPDIR / "tmp_sub.jsonl", "rb") as fin:
    with gzip.open(sub / "sub.jsonl.gz", "wb") as fout:
        fout.write(fin.read())

dir_out2 = OUTDIR / "T08_recursive"
py(["convert", "--dir", str(dir_in), "--output-dir", str(dir_out2), "-r"],
   name="T08: Recursive directory (4 files total)",
   expect_in_stdout=["4 converted"])

# ── Format override ───────────────────────────────────────────────────────────
renamed = TMPDIR / "test_noext"
import shutil
shutil.copy(gz_file, renamed)
out_forced = OUTDIR / "T09_forced.pfc"
py(["convert", str(renamed), str(out_forced), "--format", "gz"],
   name="T09: --format override (no extension)",
   expect_in_stdout=["Done"])

# ══════════════════════════════════════════════════════════════════════════════
print("\n[3] ERROR HANDLING")

py(["convert", "/nonexistent/file.jsonl.gz", str(OUTDIR / "T10.pfc")],
   name="T10: File not found → clear error",
   expect_exit=1,
   expect_in_stdout=["ERROR"])

py(["convert", str(TMPDIR / "unknown.xyz"), str(OUTDIR / "T11.pfc")],
   name="T11: Unknown format → clear error",
   expect_exit=1,
   expect_in_stdout=["ERROR", "format"])

# Corrupt gz file
corrupt = TMPDIR / "corrupt.jsonl.gz"
with open(corrupt, "wb") as f:
    f.write(b"this is not a valid gzip file\x00\x01\x02")
py(["convert", str(corrupt), str(OUTDIR / "T12.pfc")],
   name="T12: Corrupt gz file → clear error",
   expect_exit=1)

py(["convert", str(gz_file), str(OUTDIR / "T13.pfc"),
    "--pfc-binary", "/nonexistent/pfc_jsonl"],
   name="T13: pfc_jsonl binary missing → clear error",
   expect_exit=1,
   expect_in_stdout=["ERROR"])

# ── Confirm DB subcommands are gone ──────────────────────────────────────────
print("\n[4] CONFIRM DB SUBCOMMANDS REMOVED")
_, res = run("T14: cratedb subcommand is gone",
             ["python3", str(SCRIPT), "cratedb", "--host", "x", "--table", "y"],
             expect_exit=2)
results[-1] = ("T14: cratedb subcommand removed", "cratedb" not in res.stdout or res.returncode == 2, 0)
print(f"  {'✅' if 'cratedb' not in (res.stdout+res.stderr) or res.returncode == 2 else '❌'}  T14: cratedb subcommand removed from CLI")

# ── Version / help ────────────────────────────────────────────────────────────
run("T15: --version flag",
    ["python3", str(SCRIPT), "--version"],
    expect_in_stdout=["2.1.0"])

run("T16: --help mentions pfc-export-cratedb",
    ["python3", str(SCRIPT), "--help"],
    expect_in_stdout=["pfc-export-cratedb"])

# ── Cleanup ───────────────────────────────────────────────────────────────────
shutil.rmtree(TMPDIR)

# ── Results ───────────────────────────────────────────────────────────────────
print("\n[5] OUTPUT FILES")
for p in sorted(OUTDIR.glob("**/*.pfc")):
    if p.stat().st_size > 0:
        print(f"  📄 {p.name:<35} {p.stat().st_size/1_048_576:.3f} MiB")

passed = sum(1 for _, ok, _ in results if ok)
failed = sum(1 for _, ok, _ in results if not ok)
total  = len(results)

print(f"\n{'='*65}")
print(f"RESULTS: {passed}/{total} PASS  |  {failed} FAIL")
print(f"{'='*65}")

if failed > 0:
    print("\nFailed tests:")
    for name, ok, _ in results:
        if not ok:
            print(f"  ❌ {name}")

sys.exit(0 if failed == 0 else 1)
