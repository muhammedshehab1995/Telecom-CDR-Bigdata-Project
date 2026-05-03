"""
upload_to_s3.py
───────────────
Uploads local CDR data files (Parquet / CSV) to the AWS S3 data lake.

Usage
-----
  python upload_to_s3.py --source ./data --bucket cdr-egypt-data-lake-dev

Requirements
------------
  pip install boto3 tqdm

Environment variables (no hard-coded credentials)
--------------------------------------------------
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_DEFAULT_REGION   (optional, defaults to me-south-1)
"""

import argparse
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None  # progress bar is optional


# ── Zone mapping: local folder name → S3 prefix ──────────────────────────────
ZONE_MAP = {
    "raw": "raw",
    "clean": "clean",
    "analytics": "analytics",
}


def get_s3_client(region: str) -> boto3.client:
    """Return a boto3 S3 client (credentials from env vars)."""
    return boto3.client("s3", region_name=region)


def upload_file(s3_client, local_path: Path, bucket: str, s3_key: str) -> bool:
    """Upload a single file to S3. Returns True on success."""
    try:
        s3_client.upload_file(str(local_path), bucket, s3_key)
        return True
    except (BotoCoreError, ClientError) as exc:
        print(f"  ✗ Failed to upload {local_path.name}: {exc}", file=sys.stderr)
        return False


def collect_files(source_dir: Path) -> list[tuple[Path, str]]:
    """
    Walk source_dir and return (local_path, s3_zone_prefix) tuples.
    Supported extensions: .parquet, .csv, .json
    """
    supported = {".parquet", ".csv", ".json"}
    files = []

    for fp in source_dir.rglob("*"):
        if fp.suffix.lower() not in supported or not fp.is_file():
            continue

        # Determine zone from parent folder name
        zone = None
        for part in fp.parts:
            if part in ZONE_MAP:
                zone = ZONE_MAP[part]
                break
        if zone is None:
            zone = "raw"  # default: treat unknown folders as raw

        relative = fp.relative_to(source_dir)
        s3_key = f"{zone}/{relative}"
        files.append((fp, s3_key))

    return files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload CDR data files to AWS S3 data lake"
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path("./data"),
        help="Local directory containing CDR data (default: ./data)",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=os.getenv("CDR_S3_BUCKET", "cdr-egypt-data-lake-dev"),
        help="Target S3 bucket name",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=os.getenv("AWS_DEFAULT_REGION", "me-south-1"),
        help="AWS region (default: me-south-1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print files that would be uploaded without actually uploading",
    )
    args = parser.parse_args()

    if not args.source.exists():
        print(f"✗ Source directory not found: {args.source}", file=sys.stderr)
        sys.exit(1)

    # ── Collect files ─────────────────────────────────────────────────────────
    print(f"🔍 Scanning {args.source} for CDR files …")
    files = collect_files(args.source)

    if not files:
        print("⚠  No supported files found (.parquet / .csv / .json). Exiting.")
        sys.exit(0)

    print(f"📦 Found {len(files)} file(s) to upload → s3://{args.bucket}/")

    if args.dry_run:
        for fp, key in files:
            print(f"  [DRY-RUN] {fp} → s3://{args.bucket}/{key}")
        return

    # ── Upload ────────────────────────────────────────────────────────────────
    s3 = get_s3_client(args.region)
    success, failed = 0, 0

    iterator = tqdm(files, unit="file") if tqdm else files
    for fp, s3_key in iterator:
        label = f"  ↑ {fp.name} → {s3_key}"
        if tqdm is None:
            print(label)
        ok = upload_file(s3, fp, args.bucket, s3_key)
        if ok:
            success += 1
        else:
            failed += 1

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n✅ Upload complete: {success} succeeded, {failed} failed.")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
