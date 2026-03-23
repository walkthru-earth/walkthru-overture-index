"""Geocoder-specific export logic for the addresses theme.

Handles the flat-file tile export that DuckDB's PARTITION_BY can't
produce natively:

  1. DuckDB COPY with PARTITION_BY → local scratch (Hive dirs)
  2. Flatten: h3_parent=XXX/data_0.parquet → h3_parent=XXX.parquet
     (multi-file partitions are concatenated via pyarrow)
  3. Parallel upload to S3 via boto3 s3transfer

This is the only theme that needs post-processing — all other themes
write directly to S3 via DuckDB httpfs.
"""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path

from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

log = logging.getLogger(__name__)


def flatten_partitions(scratch_dir: str) -> int:
    """Flatten Hive h3_parent directories into flat tile files.

    DuckDB PARTITION_BY writes:
      geocoder/country=US/h3_parent=842a1.../data_0.parquet

    We flatten to:
      geocoder/country=US/h3/842a1....parquet

    DuckDB may split large partitions into multiple files. Any
    multi-file partition is concatenated into one via pyarrow.

    Returns the number of directories flattened.
    """
    geocoder_dir = Path(scratch_dir) / "geocoder"
    if not geocoder_dir.exists():
        return 0

    count = 0
    merged = 0
    for country_dir in sorted(geocoder_dir.iterdir()):
        if not country_dir.is_dir():
            continue
        for h3_dir in sorted(country_dir.iterdir()):
            if not h3_dir.is_dir():
                continue
            # Extract h3 hex from "h3_parent=842a1...ffffffff"
            h3_hex = h3_dir.name.removeprefix("h3_parent=")
            h3_subdir = country_dir / "h3"
            h3_subdir.mkdir(exist_ok=True)
            dest = h3_subdir / f"{h3_hex}.parquet"
            parquet_files = sorted(h3_dir.glob("*.parquet"))
            if len(parquet_files) == 1:
                shutil.move(str(parquet_files[0]), str(dest))
                h3_dir.rmdir()
                count += 1
            elif parquet_files:
                tables = [pq.read_table(str(f)) for f in parquet_files]
                combined = pa.concat_tables(tables)
                pq.write_table(
                    combined,
                    str(dest),
                    compression="zstd",
                    compression_level=6,
                    row_group_size=50000,
                    version="2.6",
                )
                shutil.rmtree(str(h3_dir))
                count += 1
                merged += 1
    if merged:
        log.info("[FLATTEN] Concatenated %d multi-file partitions", merged)
    return count


UPLOAD_WORKERS = 64
UPLOAD_POOL_CONNECTIONS = UPLOAD_WORKERS + 10


def _upload_one(
    s3_client: object,
    local_path: Path,
    bucket: str,
    key: str,
    transfer_config: TransferConfig,
) -> str:
    """Upload a single file. Called from worker threads."""
    s3_client.upload_file(str(local_path), bucket, key, Config=transfer_config)
    return key


def upload_tiles(scratch_dir: str, s3_bucket: str, s3_prefix: str) -> int:
    """Upload flattened geocoder tiles to S3 using parallel boto3 transfers.

    Uses ThreadPoolExecutor for file-level parallelism (64 files at once)
    and TransferConfig for per-file multipart chunk parallelism.

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
        "[UPLOAD] %d tile files → s3://%s/%s/geocoder/ (%d workers)",
        total,
        s3_bucket,
        s3_prefix,
        UPLOAD_WORKERS,
    )

    s3 = boto3.client(
        "s3",
        config=Config(
            max_pool_connections=UPLOAD_POOL_CONNECTIONS,
            retries={"max_attempts": 5, "mode": "adaptive"},
        ),
    )
    transfer_cfg = TransferConfig(
        multipart_threshold=16 * 1024 * 1024,
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=4,
        use_threads=True,
    )

    t0 = time.time()
    done = 0
    failed = []

    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as pool:
        futures = {}
        for f in files:
            rel = f.relative_to(Path(scratch_dir))
            key = f"{s3_prefix}/{rel}"
            fut = pool.submit(_upload_one, s3, f, s3_bucket, key, transfer_cfg)
            futures[fut] = key

        for fut in as_completed(futures):
            key = futures[fut]
            try:
                fut.result()
                done += 1
                if done % 1000 == 0:
                    elapsed = time.time() - t0
                    log.info("[UPLOAD] %d/%d files (%.1fs)", done, total, elapsed)
            except Exception as e:
                log.error("[UPLOAD] FAILED %s: %s", key, e)
                failed.append(key)

    elapsed = time.time() - t0
    log.info("[UPLOAD] %d files uploaded in %.1fs", done, elapsed)
    if failed:
        log.error("[UPLOAD] %d files failed", len(failed))
    return done


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
