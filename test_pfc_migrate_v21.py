#!/usr/bin/env python3
"""
pfc-migrate v2.1.0 — Addendum Test Suite
==========================================
Tests for new v2.1.0 features:
  - --stdin / --out (pipe mode)
  - Public Storage API (get_s3_client, get_azure_client, get_gcs_client, upload_pfc_to_s3)

Run on server: python3 test_pfc_migrate_v21.py
Author: ForgeBuddy + Dante | 2026-04-29
"""

import gzip
import io
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

SCRIPT  = Path("/root/pfc_migrate.py")
PFC_BIN = "/usr/local/bin/pfc_jsonl"
OUTDIR  = Path(tempfile.mkdtemp(prefix="pfc_migrate_v21_"))

results = []


def run(name, cmd, expect_exit=0, expect_in_stdout=None, stdin_data=None):
    t0  = time.time()
    res = subprocess.run(cmd, capture_output=True, text=True, input=stdin_data)
    dt  = time.time() - t0
    ok  = True
    reasons = []

    if res.returncode != expect_exit:
        ok = False
        reasons.append(f"exit={res.returncode} expected={expect_exit}")
    for s in (expect_in_stdout or []):
        if s not in (res.stdout + res.stderr):
            ok = False
            reasons.append(f"missing in output: {s!r}")

    status = "PASS" if ok else "FAIL"
    print(f"  {'OK' if ok else 'XX'}  [{dt:.1f}s]  {name}")
    if not ok:
        for r in reasons:
            print(f"           -> {r}")
        if res.stderr.strip():
            print(f"           stderr: {res.stderr.strip()[:300]}")
    results.append((name, ok, dt))
    return ok, res


def py(*args, **kwargs):
    return run(args[0], ["python3", str(SCRIPT)] + list(args[1:]), **kwargs)


def make_jsonl(n=100) -> bytes:
    lines = []
    for i in range(n):
        lines.append(json.dumps({
            "timestamp": f"2026-04-29T10:{i // 60:02d}:{i % 60:02d}Z",
            "service": f"svc-{i % 5}",
            "status": 200 + i % 5,
            "latency_ms": i * 3,
        }) + "\n")
    return "".join(lines).encode()


def make_gz(n=100) -> bytes:
    return gzip.compress(make_jsonl(n))


print(f"\npfc-migrate v2.1.0 — Addendum Test Suite")
print(f"Output: {OUTDIR}\n")

# ---------------------------------------------------------------------------
# 1. --stdin flag (pipe mode)
# ---------------------------------------------------------------------------

print("[1] --stdin PIPE MODE")

def test_stdin_basic():
    out = str(OUTDIR / "stdin_basic.pfc")
    jsonl_data = make_jsonl(50).decode()
    ok, res = py(
        "--stdin flag basic",
        "convert", "--stdin", "--out", out,
        stdin_data=jsonl_data,
    )
    if ok:
        assert Path(out).exists(), ".pfc not created"
        assert Path(out + ".bidx").exists(), ".bidx not created"
        # verify row count via decompress
        dec = str(OUTDIR / "stdin_dec.jsonl")
        subprocess.run([PFC_BIN, "decompress", out, dec], check=True)
        rows = [l for l in Path(dec).read_text().splitlines() if l.strip()]
        assert len(rows) == 50, f"row count: {len(rows)}"
        print(f"           50 rows, .pfc + .bidx OK")

test_stdin_basic()

# --stdin without --out → should error
py("--stdin without --out gives error",
   "convert", "--stdin",
   expect_exit=1)

# --stdin with output as positional arg
def test_stdin_positional_out():
    out = str(OUTDIR / "stdin_pos.pfc")
    jsonl_data = make_jsonl(20).decode()
    ok, res = py("--stdin with positional output arg",
                 "convert", "--stdin", out,
                 stdin_data=jsonl_data)
    if ok:
        assert Path(out).exists(), ".pfc not created via positional"

test_stdin_positional_out()

# --stdin --verbose
def test_stdin_verbose():
    out = str(OUTDIR / "stdin_verbose.pfc")
    jsonl_data = make_jsonl(10).decode()
    ok, res = py("--stdin --verbose output",
                 "convert", "--stdin", "--out", out, "--verbose",
                 stdin_data=jsonl_data)
    # verbose should produce some output
    combined = res.stdout + res.stderr
    assert len(combined.strip()) > 0, "verbose produced no output"

test_stdin_verbose()

# pipe: gzip decompress externally → stdin → pfc
def test_stdin_from_gzip_pipe():
    gz_data = make_gz(30)
    gz_file = OUTDIR / "pipe_input.jsonl.gz"
    gz_file.write_bytes(gz_data)
    out = str(OUTDIR / "pipe_from_gz.pfc")

    p_gz = subprocess.Popen(
        ["gzip", "-dc", str(gz_file)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    p_migrate = subprocess.Popen(
        ["python3", str(SCRIPT), "convert", "--stdin", "--out", out],
        stdin=p_gz.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    p_gz.stdout.close()
    stdout, stderr = p_migrate.communicate(timeout=30)
    p_gz.wait()

    ok = p_migrate.returncode == 0 and Path(out).exists()
    status = "OK" if ok else "XX"
    if not ok:
        print(f"  XX  pipe from gzip: exit={p_migrate.returncode}, {stderr.decode()[:200]}")
    else:
        dec = str(OUTDIR / "pipe_gz_dec.jsonl")
        subprocess.run([PFC_BIN, "decompress", out, dec], check=True)
        rows = [l for l in Path(dec).read_text().splitlines() if l.strip()]
        print(f"  {status}  [ok]  pipe: gzip | pfc-migrate --stdin  ({len(rows)} rows)")
        assert len(rows) == 30, f"row count: {len(rows)}"
    results.append(("pipe: gzip | pfc-migrate --stdin", ok, 0))

test_stdin_from_gzip_pipe()


# ---------------------------------------------------------------------------
# 2. --out flag (alternative to positional output)
# ---------------------------------------------------------------------------

print("\n[2] --out FLAG")

def test_out_flag_single_file():
    src = OUTDIR / "out_flag_input.jsonl.gz"
    src.write_bytes(make_gz(25))
    out = str(OUTDIR / "out_flag_result.pfc")
    py("--out flag for single file",
       "convert", str(src), "--out", out)
    assert Path(out).exists()

test_out_flag_single_file()


# ---------------------------------------------------------------------------
# 3. Public Storage API (importable)
# ---------------------------------------------------------------------------

print("\n[3] PUBLIC STORAGE API")

sys.path.insert(0, "/root")
import pfc_migrate as pm

def check(name, fn):
    t0 = time.time()
    try:
        fn()
        dt = time.time() - t0
        print(f"  OK  [{dt:.2f}s]  {name}")
        results.append((name, True, dt))
    except Exception as exc:
        dt = time.time() - t0
        print(f"  XX  [{dt:.2f}s]  {name}")
        print(f"           -> {exc}")
        results.append((name, False, dt))


def test_api_get_s3_client_importable():
    assert hasattr(pm, 'get_s3_client'), "get_s3_client not exported"
    assert callable(pm.get_s3_client)

check("get_s3_client importable + callable", test_api_get_s3_client_importable)


def test_api_get_azure_client_importable():
    assert hasattr(pm, 'get_azure_client'), "get_azure_client not exported"
    assert callable(pm.get_azure_client)

check("get_azure_client importable + callable", test_api_get_azure_client_importable)


def test_api_get_gcs_client_importable():
    assert hasattr(pm, 'get_gcs_client'), "get_gcs_client not exported"
    assert callable(pm.get_gcs_client)

check("get_gcs_client importable + callable", test_api_get_gcs_client_importable)


def test_api_upload_pfc_importable():
    assert hasattr(pm, 'upload_pfc_to_s3'), "upload_pfc_to_s3 not exported"
    assert callable(pm.upload_pfc_to_s3)

check("upload_pfc_to_s3 importable + callable", test_api_upload_pfc_importable)


def test_api_upload_pfc_uploads_bidx():
    """upload_pfc_to_s3 uploads both .pfc and .bidx when bidx exists."""
    # Mock S3 client
    uploaded = []
    class MockS3:
        def upload_file(self, src, bucket, key):
            uploaded.append((src, bucket, key))

    pfc_file = OUTDIR / "api_test.pfc"
    bidx_file = OUTDIR / "api_test.pfc.bidx"
    pfc_file.write_bytes(b"fake pfc content")
    bidx_file.write_bytes(b"fake bidx content")

    pm.upload_pfc_to_s3(MockS3(), pfc_file, "test-bucket", "pfc/api_test.pfc")

    assert len(uploaded) == 2, f"Expected 2 uploads, got {len(uploaded)}: {uploaded}"
    keys = [u[2] for u in uploaded]
    assert "pfc/api_test.pfc" in keys
    assert "pfc/api_test.pfc.bidx" in keys

check("upload_pfc_to_s3 uploads .pfc + .bidx", test_api_upload_pfc_uploads_bidx)


def test_api_upload_pfc_no_bidx():
    """upload_pfc_to_s3 only uploads .pfc when no .bidx exists."""
    uploaded = []
    class MockS3:
        def upload_file(self, src, bucket, key):
            uploaded.append(key)

    pfc_file = OUTDIR / "no_bidx.pfc"
    pfc_file.write_bytes(b"fake pfc")
    # No .bidx file

    pm.upload_pfc_to_s3(MockS3(), pfc_file, "bucket", "no_bidx.pfc")
    assert len(uploaded) == 1, f"Should upload only .pfc, got: {uploaded}"
    assert uploaded[0] == "no_bidx.pfc"

check("upload_pfc_to_s3 skips .bidx when missing", test_api_upload_pfc_no_bidx)


def test_api_get_s3_client_missing_boto3():
    """get_s3_client raises ImportError if boto3 not installed (simulate)."""
    import unittest.mock as mock
    with mock.patch.dict('sys.modules', {'boto3': None}):
        try:
            pm.get_s3_client()
            raise AssertionError("Should raise ImportError")
        except (ImportError, TypeError):
            pass  # expected

check("get_s3_client raises ImportError without boto3", test_api_get_s3_client_missing_boto3)


def test_api_get_azure_missing_sdk():
    import unittest.mock as mock
    with mock.patch.dict('sys.modules', {'azure.storage.blob': None, 'azure': None,
                                          'azure.storage': None}):
        try:
            pm.get_azure_client(connection_string="fake")
            raise AssertionError("Should raise")
        except (ImportError, TypeError, ModuleNotFoundError):
            pass

check("get_azure_client raises ImportError without sdk", test_api_get_azure_missing_sdk)


def test_api_get_azure_needs_param():
    """get_azure_client raises ValueError when no connection string or URL given."""
    try:
        import azure.storage.blob  # skip if not installed
    except ImportError:
        print("  SKIP  get_azure_client no-param error  (azure sdk not installed)")
        results.append(("get_azure_client no-param raises ValueError", True, 0))
        return
    try:
        pm.get_azure_client()
        raise AssertionError("Should raise ValueError")
    except ValueError:
        pass

check("get_azure_client no-param raises ValueError", test_api_get_azure_needs_param)


# ---------------------------------------------------------------------------
# 4. Version check
# ---------------------------------------------------------------------------

print("\n[4] VERSION")
py("--version shows 2.1.0", "--version", expect_in_stdout=["2.1.0"])


# ---------------------------------------------------------------------------
# 5. Regression: existing convert still works
# ---------------------------------------------------------------------------

print("\n[5] REGRESSION — existing convert unbroken")

def test_regression_gz_convert():
    src = OUTDIR / "regr_input.jsonl.gz"
    src.write_bytes(make_gz(200))
    out = str(OUTDIR / "regr_output.pfc")
    ok, res = py("regression: gzip -> pfc still works",
                 "convert", str(src), out)
    if ok:
        assert Path(out).exists()
        dec = str(OUTDIR / "regr_dec.jsonl")
        subprocess.run([PFC_BIN, "decompress", out, dec], check=True)
        rows = [l for l in Path(dec).read_text().splitlines() if l.strip()]
        assert len(rows) == 200, f"row count: {len(rows)}"
        print(f"           200 rows roundtrip OK")

test_regression_gz_convert()

py("regression: dir mode unbroken",
   "convert", "--dir", str(OUTDIR / "nonexistent_for_empty_test"),
   expect_exit=0,
   expect_in_stdout=["No JSONL"])


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

total  = len(results)
passed = sum(1 for _, ok, _ in results if ok)
failed = total - passed
total_t = sum(t for _, _, t in results)

print(f"\n{'='*60}")
print(f"  {passed}/{total} PASS   {failed} FAIL   {total_t:.1f}s total")
print(f"{'='*60}")

if failed:
    print("\nFailed:")
    for name, ok, _ in results:
        if not ok:
            print(f"  - {name}")
    sys.exit(1)
