"""Geocoder-specific export logic for the addresses theme.

Handles the flat-file tile export that DuckDB's PARTITION_BY can't
produce natively:

  1. DuckDB COPY with PARTITION_BY → local scratch (Hive dirs)
  2. Flatten: h3_parent=XXX/data_0.parquet → h3/XXX.parquet
     (multi-file partitions are concatenated via pyarrow)
  3. Flatten index partitions: country=XX/data_0.parquet → XX.parquet
  4. Parallel upload ALL scratch files to S3 via boto3

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


def flatten_geocoder_tiles(scratch_dir: str) -> int:
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
            if not parquet_files:
                continue
            # Read, sort by h3_index, and write — ensures row groups
            # have contiguous h3_index ranges for filter pushdown.
            # DuckDB PARTITION_BY doesn't preserve ORDER BY within partitions.
            tables = [pq.read_table(str(f)) for f in parquet_files]
            combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
            has_h3 = "h3_index" in combined.column_names
            if has_h3:
                combined = combined.sort_by("h3_index")

            # Build sorting_columns metadata so readers know the sort order
            sorting_cols = None
            if has_h3:
                sorting_cols = pq.SortingColumn.from_ordering(
                    combined.schema, [("h3_index", "ascending")]
                )

            pq.write_table(
                combined,
                str(dest),
                compression="zstd",
                compression_level=6,
                row_group_size=50000,
                version="2.6",
                write_statistics=True,
                write_page_index=True,
                sorting_columns=sorting_cols,
                data_page_size=1024 * 1024,
                # TODO: Add bloom filters on street when PyArrow gains write support
                # (PR apache/arrow#49377, expected in PyArrow 24+).
                # Bloom filters would enable row-group skipping for direct
                # forward-geocode queries (WHERE lower(street) = 'x').
            )
            shutil.rmtree(str(h3_dir))
            count += 1
            if len(parquet_files) > 1:
                merged += 1
    if merged:
        log.info("[FLATTEN] Concatenated %d multi-file geocoder partitions", merged)
    return count


def flatten_index_partitions(
    scratch_dir: str,
    index_name: str,
    *,
    row_group_size: int | None = None,
    bloom_filter_columns: list[str] | None = None,
    sorting_columns: list[tuple[str, str]] | None = None,
) -> int:
    """Flatten a Hive-partitioned index into flat per-country files.

    DuckDB PARTITION_BY writes:
      {index_name}/country=NL/data_0.parquet

    We flatten to:
      {index_name}/NL.parquet

    Optional params for on-demand indexes (e.g. number_index) that are
    queried via HTTP range requests instead of bulk-loaded into WASM:
      row_group_size: small row groups enable row-group pushdown
      bloom_filter_columns: bloom filters on lookup columns skip non-matching groups
      sorting_columns: list of (column, "ascending"/"descending") for sort metadata

    Returns the number of country files created.
    """
    index_dir = Path(scratch_dir) / index_name
    if not index_dir.exists():
        return 0

    count = 0
    for country_dir in sorted(index_dir.iterdir()):
        if not country_dir.is_dir():
            continue
        cc = country_dir.name.removeprefix("country=")
        parquet_files = sorted(country_dir.glob("*.parquet"))
        if not parquet_files:
            continue

        dest = index_dir / f"{cc}.parquet"
        tables = [pq.read_table(str(f)) for f in parquet_files]
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

        # Build sorting metadata if requested
        sort_cols = None
        if sorting_columns:
            sort_cols = pq.SortingColumn.from_ordering(combined.schema, sorting_columns)

        write_kwargs: dict = {
            "compression": "zstd",
            "compression_level": 6,
            "version": "2.6",
            "write_statistics": True,
            "write_page_index": True,
        }
        if row_group_size is not None:
            write_kwargs["row_group_size"] = row_group_size
        # TODO: Add bloom filters when PyArrow gains write support
        # (PR apache/arrow#49377, expected in PyArrow 24+).
        # if bloom_filter_columns:
        #     write_kwargs["bloom_filter_options"] = {col: True for col in bloom_filter_columns}
        if sort_cols is not None:
            write_kwargs["sorting_columns"] = sort_cols

        pq.write_table(combined, str(dest), **write_kwargs)
        shutil.rmtree(str(country_dir))
        count += 1

    log.info("[FLATTEN] %s: %d per-country files", index_name, count)
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


def upload_all(scratch_dir: str, s3_bucket: str, s3_prefix: str) -> int:
    """Upload ALL parquet files under scratch_dir to S3.

    Uploads geocoder tiles, per-country index files, and any other
    parquet files written to the scratch directory.

    Uses ThreadPoolExecutor for file-level parallelism (64 files at once)
    and TransferConfig for per-file multipart chunk parallelism.

    Returns the number of files uploaded.
    """
    scratch = Path(scratch_dir)
    files = sorted(scratch.rglob("*.parquet"))
    total = len(files)
    if not total:
        log.warning("[UPLOAD] No parquet files in %s", scratch_dir)
        return 0

    log.info(
        "[UPLOAD] %d files → s3://%s/%s/ (%d workers)",
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
            rel = f.relative_to(scratch)
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
    """Flatten local partitions and upload everything to S3.

    Called by main.py after DuckDB writes to scratch_dir:
      - geocoder/country=XX/h3_parent=YYY/  → geocoder/country=XX/h3/YYY.parquet
      - postcode_index/country=XX/           → postcode_index/XX.parquet
      - street_index/country=XX/             → street_index/XX.parquet
      - city_index/country=XX/               → city_index/XX.parquet
      - number_index/country=XX/             → number_index/XX.parquet
    """
    t0 = time.time()

    # Flatten geocoder tiles (h3_parent dirs → flat files)
    n_tiles = flatten_geocoder_tiles(scratch_dir)
    log.info("[GEOCODER] Flattened %d tile dirs in %.1fs", n_tiles, time.time() - t0)

    # Flatten per-country index partitions
    n_postcode = flatten_index_partitions(scratch_dir, "postcode_index")
    n_street = flatten_index_partitions(scratch_dir, "street_index")
    n_city = flatten_index_partitions(scratch_dir, "city_index")
    # number_index is queried on-demand via HTTP range requests (not bulk-loaded).
    # Small row groups (2000) + sorting enable DuckDB-WASM row-group pushdown,
    # fetching only ~100-150 KB per query instead of the full file (7-12 MB).
    # bloom_filter_columns is accepted but no-op until PyArrow 24+ (see TODO in flatten).
    n_number = flatten_index_partitions(
        scratch_dir,
        "number_index",
        row_group_size=2000,
        bloom_filter_columns=["street_lower"],
        sorting_columns=[("street_lower", "ascending")],
    )
    log.info(
        "[GEOCODER] Index partitions: %d postcode + %d street + %d city + %d number countries",
        n_postcode,
        n_street,
        n_city,
        n_number,
    )

    # Upload everything in scratch_dir
    upload_all(scratch_dir, s3_bucket, s3_prefix)

    # Cleanup scratch directory
    shutil.rmtree(scratch_dir, ignore_errors=True)
    log.info("[GEOCODER] Total export+upload: %.1fs", time.time() - t0)
