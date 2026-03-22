"""Geocoder-specific export logic for the addresses theme.

Handles the flat-file tile export that DuckDB's PARTITION_BY can't
produce natively:

  1. DuckDB COPY with PARTITION_BY → local scratch (Hive dirs)
  2. Flatten: h3_parent=XXX/data_0.parquet → h3_parent=XXX.parquet
  3. Parallel upload to S3 via boto3 s3transfer

This is the only theme that needs post-processing — all other themes
write directly to S3 via DuckDB httpfs.
"""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig

log = logging.getLogger(__name__)


def flatten_partitions(scratch_dir: str) -> int:
    """Flatten Hive h3_parent directories into flat Parquet files.

    Renames:
      geocoder/country=US/h3_parent=842a.../data_0.parquet
    To:
      geocoder/country=US/h3_parent=842a....parquet

    Returns the number of files flattened.
    """
    geocoder_dir = Path(scratch_dir) / "geocoder"
    if not geocoder_dir.exists():
        return 0

    count = 0
    for country_dir in sorted(geocoder_dir.iterdir()):
        if not country_dir.is_dir():
            continue
        for h3_dir in sorted(country_dir.iterdir()):
            if not h3_dir.is_dir():
                continue
            parquet_files = list(h3_dir.glob("*.parquet"))
            if len(parquet_files) == 1:
                dest = h3_dir.with_suffix(".parquet")
                shutil.move(str(parquet_files[0]), str(dest))
                h3_dir.rmdir()
                count += 1
            elif parquet_files:
                log.warning(
                    "[FLATTEN] %s has %d files, skipping",
                    h3_dir,
                    len(parquet_files),
                )
    return count


def upload_tiles(scratch_dir: str, s3_bucket: str, s3_prefix: str) -> int:
    """Upload flattened geocoder tiles to S3 using boto3 parallel transfers.

    Uses S3 transfer manager for concurrent multipart uploads.
    ~4,000-6,000 files at ~1 MB each = ~5-20 GB total.

    Returns the number of files uploaded.
    """
    geocoder_dir = Path(scratch_dir) / "geocoder"
    if not geocoder_dir.exists():
        log.warning("[UPLOAD] No geocoder/ directory in %s", scratch_dir)
        return 0

    files = sorted(geocoder_dir.rglob("*.parquet"))
    total = len(files)
    if not total:
        return 0

    log.info(
        "[UPLOAD] %d tile files → s3://%s/%s/geocoder/",
        total,
        s3_bucket,
        s3_prefix,
    )

    s3 = boto3.client("s3")
    # Aggressive concurrency: 50 threads, 8 MB chunks
    config = TransferConfig(
        max_concurrency=50,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True,
    )

    t0 = time.time()
    count = 0
    for f in files:
        rel = f.relative_to(Path(scratch_dir))
        key = f"{s3_prefix}/{rel}"
        s3.upload_file(
            str(f),
            s3_bucket,
            key,
            Config=config,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )
        count += 1
        if count % 500 == 0:
            elapsed = time.time() - t0
            log.info("[UPLOAD] %d/%d files (%.1fs)", count, total, elapsed)

    elapsed = time.time() - t0
    log.info("[UPLOAD] %d files uploaded in %.1fs", count, elapsed)
    return count


def export_and_upload(
    scratch_dir: str,
    s3_bucket: str,
    s3_prefix: str,
) -> None:
    """Flatten local Hive-partitioned tiles and upload to S3.

    Called by main.py after DuckDB COPY writes tiles to scratch_dir.
    """
    t0 = time.time()

    n = flatten_partitions(scratch_dir)
    log.info("[GEOCODER] Flattened %d partition dirs in %.1fs", n, time.time() - t0)

    upload_tiles(scratch_dir, s3_bucket, s3_prefix)

    shutil.rmtree(scratch_dir, ignore_errors=True)
    log.info("[GEOCODER] Total export+upload: %.1fs", time.time() - t0)
