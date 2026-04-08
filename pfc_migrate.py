#!/usr/bin/env python3
"""
pfc-migrate — Convert compressed JSONL archives to PFC format.

Supports:
  Input formats : gzip (.gz), zstd (.zst), bzip2 (.bz2), lz4 (.lz4), plain JSONL
  Storage       : Local filesystem (Stage 1)
                  S3 / S3 Glacier   (Stage 2) ← NEW
                  Azure / GCS       (Stage 3, planned)

Usage:
  pfc-migrate convert logs.jsonl.gz  logs.pfc
  pfc-migrate convert --dir /var/log/archive/ --output-dir /var/log/pfc/
  pfc-migrate convert --dir /var/log/ --format gz --recursive --verbose
"""

import argparse
import bz2
import gzip
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


# ---------------------------------------------------------------------------
# Binary detection
# ---------------------------------------------------------------------------

def find_pfc_binary(override=None):
    """Locate the pfc_jsonl binary. Returns path or None."""
    if override:
        if os.path.isfile(override) and os.access(override, os.X_OK):
            return override
        raise FileNotFoundError(f"pfc_jsonl binary not found at: {override}")

    env = os.environ.get("PFC_JSONL_BINARY")
    if env and os.path.isfile(env) and os.access(env, os.X_OK):
        return env

    default = "/usr/local/bin/pfc_jsonl"
    if os.path.isfile(default) and os.access(default, os.X_OK):
        return default

    found = shutil.which("pfc_jsonl")
    if found:
        return found

    return None


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

FORMAT_EXTENSIONS = {
    ".gz":   "gz",
    ".zst":  "zst",
    ".bz2":  "bz2",
    ".lz4":  "lz4",
}

def detect_format(path: Path):
    """Detect compression format from file extension. Returns 'gz', 'zst', 'bz2', 'lz4', or 'plain'."""
    suffix = path.suffix.lower()
    if suffix in FORMAT_EXTENSIONS:
        return FORMAT_EXTENSIONS[suffix]
    if suffix in (".jsonl", ".json", ".ndjson"):
        return "plain"
    return None


def output_path_for(input_path: Path, output_dir=None) -> Path:
    """Derive the .pfc output path from an input path."""
    name = input_path.name

    # Strip compression extension if present
    for ext in FORMAT_EXTENSIONS:
        if name.lower().endswith(ext):
            name = name[: -len(ext)]
            break

    # Replace .jsonl / .json / .ndjson → .pfc, or append .pfc
    for base_ext in (".jsonl", ".json", ".ndjson"):
        if name.lower().endswith(base_ext):
            name = name[: -len(base_ext)] + ".pfc"
            break
    else:
        name = name + ".pfc"

    base = Path(output_dir) if output_dir else input_path.parent
    return base / name


# ---------------------------------------------------------------------------
# Core conversion
# ---------------------------------------------------------------------------

def _decompress_to_tmp(input_path: Path, fmt: str, tmp_path: str, verbose: bool):
    """Decompress input_path → tmp_path using the given format."""
    if fmt == "gz":
        with gzip.open(input_path, "rb") as fin, open(tmp_path, "wb") as fout:
            shutil.copyfileobj(fin, fout)

    elif fmt == "bz2":
        with bz2.open(input_path, "rb") as fin, open(tmp_path, "wb") as fout:
            shutil.copyfileobj(fin, fout)

    elif fmt == "zst":
        try:
            import zstandard as zstd
        except ImportError:
            raise ImportError(
                "The 'zstandard' package is required for .zst files.\n"
                "Install it: pip install zstandard"
            )
        dctx = zstd.ZstdDecompressor()
        with open(input_path, "rb") as fin, open(tmp_path, "wb") as fout:
            dctx.copy_stream(fin, fout)

    elif fmt == "lz4":
        try:
            import lz4.frame
        except ImportError:
            raise ImportError(
                "The 'lz4' package is required for .lz4 files.\n"
                "Install it: pip install lz4"
            )
        with lz4.frame.open(input_path, "rb") as fin, open(tmp_path, "wb") as fout:
            shutil.copyfileobj(fin, fout)

    elif fmt == "plain":
        shutil.copy2(input_path, tmp_path)

    else:
        raise ValueError(f"Unsupported format: {fmt}")


def convert_file(
    input_path,
    output_path,
    pfc_binary: str,
    fmt: str = None,
    verbose: bool = False,
) -> dict:
    """
    Convert a single compressed JSONL file to PFC format.

    Returns a dict with keys: input, output, input_mb, output_mb, ratio_pct
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fmt = fmt or detect_format(input_path)
    if not fmt:
        raise ValueError(
            f"Cannot detect format for '{input_path.name}'. "
            "Use --format to specify: gz | zst | bz2 | lz4 | plain"
        )

    if verbose:
        print(f"  → {input_path.name}  [{fmt}]")

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".jsonl")
    os.close(tmp_fd)

    try:
        # Step 1: decompress
        _decompress_to_tmp(input_path, fmt, tmp_path, verbose)

        # Step 2: pfc_jsonl compress
        result = subprocess.run(
            [pfc_binary, "compress", tmp_path, str(output_path)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"pfc_jsonl compress failed (exit {result.returncode}):\n{result.stderr.strip()}"
            )

        # Stats — compare PFC to the decompressed (original) size for honest ratio
        compressed_mb   = input_path.stat().st_size      / 1_048_576
        decompressed_mb = Path(tmp_path).stat().st_size  / 1_048_576  # size before PFC
        output_mb       = output_path.stat().st_size     / 1_048_576
        ratio_pct       = (output_mb / decompressed_mb * 100) if decompressed_mb > 0 else 0.0

        if verbose:
            fmt_label = f" [{fmt}]" if fmt != "plain" else ""
            print(
                f"     original {decompressed_mb:.1f} MB"
                f"  →  {fmt}{fmt_label} {compressed_mb:.1f} MB"
                f"  →  pfc {output_mb:.1f} MB"
                f"  ({ratio_pct:.1f}% of original)  ✓ {output_path.name}"
            )

        return {
            "input":           str(input_path),
            "output":          str(output_path),
            "compressed_mb":   compressed_mb,
            "decompressed_mb": decompressed_mb,
            "output_mb":       output_mb,
            "ratio_pct":       ratio_pct,
        }

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# Directory / batch mode
# ---------------------------------------------------------------------------

_GLOB_PATTERNS = [
    "*.jsonl.gz",  "*.json.gz",  "*.ndjson.gz",
    "*.jsonl.zst", "*.json.zst", "*.ndjson.zst",
    "*.jsonl.bz2", "*.json.bz2", "*.ndjson.bz2",
    "*.jsonl.lz4", "*.json.lz4", "*.ndjson.lz4",
    "*.jsonl",     "*.json",     "*.ndjson",
]


def convert_dir(
    input_dir,
    output_dir=None,
    fmt: str = None,
    pfc_binary: str = None,
    verbose: bool = False,
    recursive: bool = False,
) -> tuple:
    """
    Convert all matching JSONL archives in a directory to PFC format.
    Returns (success_count, failed_count).
    """
    input_dir = Path(input_dir)
    files = []

    for pattern in _GLOB_PATTERNS:
        glob_fn = input_dir.rglob if recursive else input_dir.glob
        for f in glob_fn(pattern):
            if f not in files:
                files.append(f)

    files = sorted(set(files))

    if not files:
        print(f"No JSONL files found in {input_dir}")
        return 0, 0

    print(f"Found {len(files)} file(s) to convert\n")

    total_decompressed_mb = total_out_mb = 0.0
    success = failed = 0

    for f in files:
        out = output_path_for(f, output_dir)
        try:
            stats = convert_file(f, out, pfc_binary, fmt=fmt, verbose=verbose)
            total_decompressed_mb += stats["decompressed_mb"]
            total_out_mb          += stats["output_mb"]
            success += 1
        except Exception as exc:
            print(f"  ERROR {f.name}: {exc}", file=sys.stderr)
            failed += 1

    # Summary
    if success:
        overall_ratio = (total_out_mb / total_decompressed_mb * 100) if total_decompressed_mb > 0 else 0
        saved_mb = total_decompressed_mb - total_out_mb
        print(
            f"\nDone: {success} converted, {failed} failed\n"
            f"  Original (decompressed) : {total_decompressed_mb:.1f} MB\n"
            f"  PFC output              : {total_out_mb:.1f} MB  ({overall_ratio:.1f}% of original)\n"
            f"  Saved vs original       : {saved_mb:.1f} MB"
        )
    else:
        print(f"\nDone: 0 converted, {failed} failed")

    return success, failed


# ---------------------------------------------------------------------------
# S3 / Glacier support (Stage 2)
# ---------------------------------------------------------------------------

def _s3_client(args):
    """Create a boto3 S3 client from CLI args."""
    try:
        import boto3
    except ImportError:
        print("ERROR: boto3 required for S3 support: pip install boto3", file=sys.stderr)
        sys.exit(1)

    kwargs = dict(region_name=args.region)
    if args.endpoint_url:
        kwargs["endpoint_url"] = args.endpoint_url
    if args.access_key:
        kwargs["aws_access_key_id"]     = args.access_key
        kwargs["aws_secret_access_key"] = args.secret_key

    return boto3.client("s3", **kwargs)


def s3_list_objects(s3, bucket: str, prefix: str) -> list:
    """List all objects under bucket/prefix. Returns list of keys."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def s3_convert_file(
    s3,
    bucket: str,
    key: str,
    out_bucket: str,
    out_prefix: str,
    pfc_binary: str,
    fmt: str = None,
    verbose: bool = False,
    delete_original: bool = False,
) -> dict:
    """
    Download one S3 object, convert to PFC, upload back.
    Returns stats dict or raises on error.
    """
    src_path = Path(key)
    fmt = fmt or detect_format(src_path)
    if not fmt:
        raise ValueError(f"Cannot detect format for '{key}'. Use --format.")

    # Derive output key
    out_name = output_path_for(src_path).name          # e.g. logs.pfc
    out_key  = (out_prefix.rstrip("/") + "/" + out_name) if out_prefix else out_name

    if verbose:
        print(f"  → s3://{bucket}/{key}  [{fmt}]")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_input  = Path(tmpdir) / src_path.name
        tmp_pfc    = Path(tmpdir) / out_name

        # 1 — Download
        if verbose:
            print(f"     Downloading ...", end=" ", flush=True)
        s3.download_file(bucket, key, str(tmp_input))
        input_mb = tmp_input.stat().st_size / 1_048_576
        if verbose:
            print(f"{input_mb:.1f} MB")

        # 2 — Convert locally
        stats = convert_file(tmp_input, tmp_pfc, pfc_binary, fmt=fmt, verbose=False)
        pfc_mb = tmp_pfc.stat().st_size / 1_048_576

        # 3 — Upload .pfc
        if verbose:
            print(f"     Uploading  s3://{out_bucket}/{out_key} ...", end=" ", flush=True)
        s3.upload_file(str(tmp_pfc), out_bucket, out_key)
        if verbose:
            print(f"{pfc_mb:.1f} MB")

        # 4 — Upload .pfc.bidx
        bidx_local = Path(str(tmp_pfc) + ".bidx")
        if bidx_local.exists():
            bidx_key = out_key + ".bidx"
            s3.upload_file(str(bidx_local), out_bucket, bidx_key)
            if verbose:
                print(f"     Uploading  s3://{out_bucket}/{bidx_key}  (index)")

        # 5 — Optionally delete original
        if delete_original:
            s3.delete_object(Bucket=bucket, Key=key)
            if verbose:
                print(f"     Deleted    s3://{bucket}/{key}")

        ratio = pfc_mb / stats["decompressed_mb"] * 100 if stats["decompressed_mb"] > 0 else 0
        if verbose:
            print(
                f"     Done: original {stats['decompressed_mb']:.1f} MB  →  "
                f"pfc {pfc_mb:.1f} MB  ({ratio:.1f}% of original)  ✓"
            )

        return {**stats, "s3_key": key, "s3_out_key": out_key}


def cmd_s3(args, pfc_binary: str):
    """Handle the `s3` subcommand."""
    s3 = _s3_client(args)

    out_bucket = args.out_bucket or args.bucket
    out_prefix = args.out_prefix or args.prefix

    if args.key:
        # Single object
        try:
            s3_convert_file(
                s3, args.bucket, args.key,
                out_bucket, out_prefix,
                pfc_binary, fmt=args.format,
                verbose=True,
                delete_original=args.delete,
            )
        except Exception as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            sys.exit(1)

    else:
        # Batch: all objects under prefix
        print(f"Listing s3://{args.bucket}/{args.prefix} ...")
        keys = s3_list_objects(s3, args.bucket, args.prefix)

        # Filter by format extension if specified
        if args.format:
            ext = f".jsonl.{args.format}"
            keys = [k for k in keys if k.lower().endswith(ext) or k.lower().endswith(f".{args.format}")]

        if not keys:
            print("No matching objects found.")
            sys.exit(0)

        print(f"Found {len(keys)} object(s) to convert\n")
        success = failed = 0

        for key in keys:
            try:
                s3_convert_file(
                    s3, args.bucket, key,
                    out_bucket, out_prefix,
                    pfc_binary, fmt=args.format,
                    verbose=args.verbose,
                    delete_original=args.delete,
                )
                success += 1
            except Exception as exc:
                print(f"  ERROR {key}: {exc}", file=sys.stderr)
                failed += 1

        print(f"\nDone: {success} converted, {failed} failed")
        sys.exit(0 if failed == 0 else 1)


def cmd_glacier(args, pfc_binary: str):
    """
    Handle `glacier` subcommand.

    Glacier objects must be restored before download.
    This command:
      1. Initiates restore for all matching objects (if not already restored)
      2. Waits (or exits with status if still restoring)
      3. Converts all restored objects to PFC
    """
    s3 = _s3_client(args)

    print(f"Listing s3://{args.bucket}/{args.prefix} ...")
    keys = s3_list_objects(s3, args.bucket, args.prefix)
    if args.format:
        ext = f".{args.format}"
        keys = [k for k in keys if k.lower().endswith(ext)]

    if not keys:
        print("No matching objects found.")
        sys.exit(0)

    print(f"Found {len(keys)} object(s)\n")

    restoring = []
    ready     = []
    failed_r  = []

    for key in keys:
        try:
            head = s3.head_object(Bucket=args.bucket, Key=key)
            restore = head.get("Restore", "")
            storage = head.get("StorageClass", "STANDARD")

            if storage not in ("GLACIER", "DEEP_ARCHIVE", "GLACIER_IR"):
                # Not in Glacier — treat as normal S3
                ready.append(key)
                continue

            if "ongoing-request=\"true\"" in restore:
                print(f"  ⏳ RESTORING: {key}")
                restoring.append(key)
            elif "ongoing-request=\"false\"" in restore:
                print(f"  ✅ READY    : {key}")
                ready.append(key)
            else:
                # Not yet initiated — start restore
                print(f"  🔄 INITIATING restore: {key}")
                s3.restore_object(
                    Bucket=args.bucket,
                    Key=key,
                    RestoreRequest={
                        "Days": args.days,
                        "GlacierJobParameters": {"Tier": args.tier.capitalize()},
                    },
                )
                restoring.append(key)

        except Exception as exc:
            print(f"  ❌ ERROR {key}: {exc}")
            failed_r.append(key)

    print(f"\nStatus: {len(ready)} ready, {len(restoring)} restoring, {len(failed_r)} errors")

    if restoring:
        print(
            f"\n⏳ {len(restoring)} object(s) still restoring.\n"
            f"   Tier '{args.tier}' typically takes:\n"
            f"     Standard  : 3–5 hours\n"
            f"     Expedited : 1–5 minutes\n"
            f"     Bulk      : 5–12 hours\n"
            f"\n   Re-run this command later to convert when ready.\n"
        )
        if not ready:
            sys.exit(2)  # exit 2 = still waiting (not an error)

    if not ready:
        print("No objects ready to convert.")
        sys.exit(0)

    # Convert ready objects
    out_bucket = args.out_bucket or args.bucket
    out_prefix = args.out_prefix or args.prefix
    success = failed = 0

    for key in ready:
        try:
            s3_convert_file(
                s3, args.bucket, key,
                out_bucket, out_prefix,
                pfc_binary, fmt=args.format,
                verbose=args.verbose,
                delete_original=args.delete,
            )
            success += 1
        except Exception as exc:
            print(f"  ERROR {key}: {exc}", file=sys.stderr)
            failed += 1

    print(f"\nDone: {success} converted, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _add_common(parser):
    parser.add_argument(
        "--format", "-f",
        choices=["gz", "zst", "bz2", "lz4", "plain"],
        default=None,
        help="Force input format (default: auto-detect from file extension)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show per-file progress and size stats",
    )
    parser.add_argument(
        "--pfc-binary",
        default=None,
        metavar="PATH",
        help="Path to pfc_jsonl binary (default: auto-detect)",
    )


def build_parser():
    parser = argparse.ArgumentParser(
        prog="pfc-migrate",
        description="Convert compressed JSONL archives to PFC format.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # Single file
  pfc-migrate convert logs.jsonl.gz logs.pfc

  # Auto-detect output name
  pfc-migrate convert logs.jsonl.zst

  # Whole directory
  pfc-migrate convert --dir /var/log/archive/ --output-dir /var/log/pfc/

  # Recursive, force format, verbose
  pfc-migrate convert --dir /mnt/logs/ --format gz -r -v

Install pfc_jsonl binary first:
  curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \\
       -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
        """,
    )

    sub = parser.add_subparsers(dest="command")

    # ---- convert (Stage 1 — local) ----
    conv = sub.add_parser("convert", help="Convert compressed JSONL to PFC (local files)")
    conv.add_argument("input",  nargs="?", help="Input file")
    conv.add_argument("output", nargs="?", help="Output .pfc file (optional, auto-generated if omitted)")
    conv.add_argument("--dir",        metavar="DIR",  help="Convert all JSONL archives in DIR")
    conv.add_argument("--output-dir", metavar="DIR",  help="Output directory (used with --dir)")
    conv.add_argument("--recursive", "-r", action="store_true", help="Recurse into subdirectories")
    _add_common(conv)

    # ---- s3 (Stage 2 — Amazon S3) ----
    def _add_s3_common(p):
        p.add_argument("--bucket",      required=True, help="Source S3 bucket name")
        p.add_argument("--key",         default=None,  help="Single object key (omit for batch)")
        p.add_argument("--prefix",      default="",    help="Object prefix for batch mode")
        p.add_argument("--out-bucket",  default=None,  help="Destination bucket (default: same as source)")
        p.add_argument("--out-prefix",  default=None,  help="Destination prefix (default: same as source prefix)")
        p.add_argument("--region",      default="us-east-1", help="AWS region (default: us-east-1)")
        p.add_argument("--endpoint-url",default=None,  help="Custom S3 endpoint (e.g. MinIO: http://localhost:9000)")
        p.add_argument("--access-key",  default=None,  help="AWS access key (default: from env/~/.aws)")
        p.add_argument("--secret-key",  default=None,  help="AWS secret key")
        p.add_argument("--delete",      action="store_true", help="Delete original object after conversion")
        _add_common(p)

    s3p = sub.add_parser("s3", help="Convert S3 objects (gzip/zstd/bzip2/lz4 → pfc) in-place")
    _add_s3_common(s3p)

    # ---- glacier (Stage 2 — S3 Glacier) ----
    glp = sub.add_parser("glacier", help="Restore + convert S3 Glacier objects to PFC")
    _add_s3_common(glp)
    glp.add_argument("--tier",  default="standard",
                     choices=["standard", "expedited", "bulk"],
                     help="Glacier retrieval tier (default: standard = 3-5h)")
    glp.add_argument("--days",  type=int, default=3,
                     help="Days to keep restored copy available (default: 3)")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Locate binary
    try:
        pfc_binary = find_pfc_binary(getattr(args, "pfc_binary", None))
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    if not pfc_binary:
        print(
            "ERROR: pfc_jsonl binary not found.\n"
            "Install it with:\n"
            "  curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/"
            "download/pfc_jsonl-linux-x64 -o /usr/local/bin/pfc_jsonl && "
            "chmod +x /usr/local/bin/pfc_jsonl",
            file=sys.stderr,
        )
        sys.exit(1)

    # ---- s3 command ----
    if args.command == "s3":
        cmd_s3(args, pfc_binary)
        return

    # ---- glacier command ----
    if args.command == "glacier":
        cmd_glacier(args, pfc_binary)
        return

    # ---- convert command ----
    if args.command == "convert":

        if args.dir:
            success, failed = convert_dir(
                args.dir,
                output_dir=args.output_dir,
                fmt=args.format,
                pfc_binary=pfc_binary,
                verbose=args.verbose,
                recursive=args.recursive,
            )
            sys.exit(0 if failed == 0 else 1)

        elif args.input:
            output = args.output or str(output_path_for(Path(args.input)))
            try:
                stats = convert_file(
                    args.input, output, pfc_binary,
                    fmt=args.format, verbose=args.verbose,
                )
                if not args.verbose:
                    ratio = stats["ratio_pct"]
                    print(f"Done: {output}  ({ratio:.1f}% of input)")
            except Exception as exc:
                print(f"ERROR: {exc}", file=sys.stderr)
                sys.exit(1)

        else:
            conv_parser = build_parser()._subparsers._actions[-1].choices["convert"]
            conv_parser.print_help()
            sys.exit(1)


if __name__ == "__main__":
    main()
