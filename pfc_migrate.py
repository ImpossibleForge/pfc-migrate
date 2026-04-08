#!/usr/bin/env python3
"""
pfc-migrate — Convert compressed JSONL archives to PFC format.

Supports:
  Input formats : gzip (.gz), zstd (.zst), bzip2 (.bz2), lz4 (.lz4), plain JSONL
  Storage       : Local filesystem (Stage 1)
                  S3 / Glacier / Azure / GCS (Stage 2+)

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

        # Stats
        input_mb  = input_path.stat().st_size  / 1_048_576
        output_mb = output_path.stat().st_size / 1_048_576
        ratio_pct = (output_mb / input_mb * 100) if input_mb > 0 else 0.0

        if verbose:
            print(
                f"     {input_mb:.1f} MB → {output_mb:.1f} MB  "
                f"({ratio_pct:.1f}% of input)  ✓ {output_path.name}"
            )

        return {
            "input":      str(input_path),
            "output":     str(output_path),
            "input_mb":   input_mb,
            "output_mb":  output_mb,
            "ratio_pct":  ratio_pct,
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

    total_in_mb = total_out_mb = 0.0
    success = failed = 0

    for f in files:
        out = output_path_for(f, output_dir)
        try:
            stats = convert_file(f, out, pfc_binary, fmt=fmt, verbose=verbose)
            total_in_mb  += stats["input_mb"]
            total_out_mb += stats["output_mb"]
            success += 1
        except Exception as exc:
            print(f"  ERROR {f.name}: {exc}", file=sys.stderr)
            failed += 1

    # Summary
    if success:
        overall_ratio = (total_out_mb / total_in_mb * 100) if total_in_mb > 0 else 0
        saved_mb = total_in_mb - total_out_mb
        print(
            f"\nDone: {success} converted, {failed} failed\n"
            f"  Input  : {total_in_mb:.1f} MB\n"
            f"  Output : {total_out_mb:.1f} MB  ({overall_ratio:.1f}% of input)\n"
            f"  Saved  : {saved_mb:.1f} MB"
        )
    else:
        print(f"\nDone: 0 converted, {failed} failed")

    return success, failed


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

    # ---- convert ----
    conv = sub.add_parser("convert", help="Convert compressed JSONL to PFC")
    conv.add_argument("input",  nargs="?", help="Input file")
    conv.add_argument("output", nargs="?", help="Output .pfc file (optional, auto-generated if omitted)")
    conv.add_argument("--dir",        metavar="DIR",  help="Convert all JSONL archives in DIR")
    conv.add_argument("--output-dir", metavar="DIR",  help="Output directory (used with --dir)")
    conv.add_argument("--recursive", "-r", action="store_true", help="Recurse into subdirectories")
    _add_common(conv)

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
