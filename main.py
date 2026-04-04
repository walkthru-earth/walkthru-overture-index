"""Walkthru Overture Index Builder.

Reads Overture Maps data directly from S3 via DuckDB httpfs,
builds H3-indexed indices, writes output to S3 (Source Cooperative).

Usage:
    uv run python main.py                                    # Latest release
    uv run python main.py --release 2026-03-18.0             # Specific release
    uv run python main.py --themes transportation            # Specific theme
    uv run python main.py --dry-run                          # Show plan

Environment variables:
    S3_BUCKET          Output S3 bucket
    S3_PREFIX          Key prefix (e.g. "walkthru-earth/indices")
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION
"""

from __future__ import annotations

import json
import logging
import os
import platform
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s  %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

OVERTURE_REGION = "us-west-2"
STAC_CATALOG = "https://labs.overturemaps.org/stac/catalog.json"

INDEX_BUILDERS: dict[str, str] = {
    "transportation": "sql/transportation.sql",
    "places": "sql/places.sql",
    "buildings": "sql/buildings.sql",
    "addresses": "sql/addresses.sql",
    "addresses_v4": "sql/addresses_v4.sql",
    "base": "sql/base.sql",
}

S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "").strip("/")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


def _mem_gb() -> str:
    """Current RSS memory in GB."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return f"{rss / 1e9:.1f}GB"
    return f"{rss / 1e6:.1f}GB"


def _log_system_info() -> None:
    """Log system specs so we can diagnose runner issues."""
    log.info("=" * 60)
    log.info("System: %s %s", platform.system(), platform.machine())
    log.info("Python: %s", sys.version.split()[0])
    log.info("DuckDB: %s", duckdb.__version__)

    try:
        import multiprocessing

        log.info("CPUs:   %d", multiprocessing.cpu_count())
    except Exception:
        pass

    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    mem_kb = int(line.split()[1])
                    log.info("RAM:    %.1f GB", mem_kb / 1e6)
                    break
    except FileNotFoundError:
        log.info("RAM:    (unknown — not Linux)")

    try:
        import shutil

        total, used, free = shutil.disk_usage(".")
        log.info("Disk:   %.1f GB free / %.1f GB total", free / 1e9, total / 1e9)
    except Exception:
        pass

    log.info("=" * 60)


def detect_latest_release() -> str:
    """Query Overture STAC catalog for the latest release version."""
    log.info("[STAC] Querying %s", STAC_CATALOG)
    t0 = time.time()
    with urlopen(STAC_CATALOG) as resp:
        catalog = json.loads(resp.read())
    log.info("[STAC] Catalog fetched in %.1fs", time.time() - t0)

    for link in catalog.get("links", []):
        if link.get("rel") == "child":
            title = link.get("title", "")
            if "latest" in title.lower():
                version = link["href"].split("/")[1]
                log.info("[STAC] Latest release: %s", version)
                return version

    children = [lnk for lnk in catalog.get("links", []) if lnk.get("rel") == "child"]
    if children:
        version = children[0]["href"].split("/")[1]
        log.info("[STAC] Latest release (fallback): %s", version)
        return version

    raise RuntimeError("No releases found in STAC catalog")


def get_duckdb() -> duckdb.DuckDBPyConnection:
    """Create DuckDB 1.5 connection with extensions and performance settings."""
    log.info("[DUCKDB] Initializing DuckDB %s", duckdb.__version__)
    con = duckdb.connect()

    con.sql("SET preserve_insertion_order = false")
    con.sql("SET temp_directory = 'duckdb_temp.tmp'")

    # Memory management: cap at 80% of system RAM so OS keeps enough
    # for networking, DNS, page cache. DuckDB spills to temp_directory.
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    total_gb = int(line.split()[1]) / 1e6
                    limit_gb = max(4, int(total_gb * 0.75))
                    con.sql(f"SET memory_limit = '{limit_gb}GB'")
                    log.info(
                        "[DUCKDB] memory_limit = %dGB (of %.1fGB total)",
                        limit_gb,
                        total_gb,
                    )
                    break
    except FileNotFoundError:
        log.info("[DUCKDB] memory_limit = default (not Linux)")

    log.info("[DUCKDB] Performance settings applied")

    for ext in ("spatial", "httpfs"):
        try:
            con.load_extension(ext)
        except Exception:
            log.info("[DUCKDB] Installing extension '%s'...", ext)
            con.install_extension(ext)
            con.load_extension(ext)
        log.info("[DUCKDB] Extension '%s' loaded", ext)

    try:
        con.load_extension("h3")
    except Exception:
        log.info("[DUCKDB] Installing extension 'h3' from community...")
        con.install_extension("h3", repository="community")
        con.load_extension("h3")
    log.info("[DUCKDB] Extension 'h3' loaded")

    # Overture source bucket (public, anonymous — explicit empty credentials
    # override AWS env vars that DuckDB httpfs auto-detects)
    con.sql(f"""
        CREATE OR REPLACE SECRET overture_s3 (
            TYPE S3,
            KEY_ID '',
            SECRET '',
            REGION '{OVERTURE_REGION}',
            SCOPE 's3://overturemaps-{OVERTURE_REGION}'
        )
    """)
    log.info(
        "[DUCKDB] Anonymous S3 secret for Overture bucket (region=%s)", OVERTURE_REGION
    )

    return con


def configure_output(con: duckdb.DuckDBPyConnection) -> str:
    """Configure S3 output path. Returns s3://bucket/prefix base path.

    DuckDB no longer writes to S3 directly. All COPY TO goes to local
    scratch, then s5cmd uploads in parallel (see s5cmd_upload).
    """
    if not S3_BUCKET:
        log.info("[OUTPUT] No S3_BUCKET, will write to ./output/")
        return ""

    parts = [S3_BUCKET]
    if S3_PREFIX:
        parts.append(S3_PREFIX)
    base = "/".join(parts)
    log.info("[OUTPUT] S3 destination: s3://%s/", base)
    return f"s3://{base}"


def s5cmd_upload(local_dir: str, s3_dest: str) -> None:
    """Upload local directory to S3 using s5cmd for parallel uploads.

    s5cmd is a Go-based parallel S3 tool, ~12x faster than aws-cli.
    Install: go install github.com/peak/s5cmd/v2@latest
    Or: https://github.com/peak/s5cmd/releases
    """
    # Count files to upload
    file_count = sum(1 for _ in Path(local_dir).rglob("*.parquet"))
    total_size = sum(f.stat().st_size for f in Path(local_dir).rglob("*.parquet"))
    log.info(
        "[UPLOAD] %d parquet files (%.1f GB) → %s",
        file_count,
        total_size / 1e9,
        s3_dest,
    )

    t0 = time.time()

    # Try s5cmd first (fastest), fall back to aws cli
    s5cmd = shutil.which("s5cmd")
    if s5cmd:
        log.info("[UPLOAD] Using s5cmd (parallel)")
        result = subprocess.run(
            [
                s5cmd,
                "--numworkers",
                "256",
                "cp",
                f"{local_dir}/*",
                f"{s3_dest}/",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            log.error("[UPLOAD] s5cmd stderr: %s", result.stderr[:500])
            raise RuntimeError(f"s5cmd upload failed (exit {result.returncode})")
    else:
        log.info("[UPLOAD] s5cmd not found, falling back to aws s3 sync")
        result = subprocess.run(
            [
                "aws",
                "s3",
                "sync",
                local_dir,
                s3_dest,
                "--only-show-errors",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            log.error("[UPLOAD] aws sync stderr: %s", result.stderr[:500])
            raise RuntimeError(f"aws s3 sync failed (exit {result.returncode})")

    elapsed = time.time() - t0
    speed = total_size / 1e6 / max(elapsed, 0.1)
    log.info(
        "[UPLOAD] Done in %.1fs (%.0f MB/s)",
        elapsed,
        speed,
    )


def merge_split_partitions(scratch_dir: str) -> None:
    """Merge split partition files (data_0, data_1, ...) into single files.

    DuckDB's partitioned writer creates multiple files per partition when
    partitioned_write_max_open_files is exceeded (default 100). The frontend
    fetches data_0.parquet by exact path (no glob), so each partition must
    contain exactly one file.

    Re-writes with matching Parquet settings so bloom filters are regenerated
    (DuckDB auto-creates bloom filters on dict-encoded columns like street,
    city, postcode).

    IMPORTANT: The merge reads split files WITHOUT an explicit ORDER BY.
    Sort order is preserved because read_parquet reads files in name order
    (data_0, data_1, ...) and DuckDB's partitioned writer writes each
    partition's rows contiguously in the ORDER BY sequence from the COPY.
    This matters for v4 geocoder tiles which are street-sorted for forward
    geocoding pushdown. Do NOT add random shuffling or parallel reads here.

    ROW_GROUP_SIZE varies by index type (see inline comments).

    GEOPARQUET_VERSION 'BOTH' preserves per-row-group geo_bbox metadata,
    enabling spatial pushdown (skips row groups by bounding box).
    """
    scratch = Path(scratch_dir)
    split_dirs = [p.parent for p in scratch.rglob("data_1.parquet")]

    if not split_dirs:
        log.info("[MERGE] No split partitions found")
        return

    log.info("[MERGE] Found %d split partition(s), merging...", len(split_dirs))
    t0 = time.time()
    merge_con = duckdb.connect()
    merge_con.load_extension("spatial")

    for d in split_dirs:
        parts = sorted(d.glob("data_*.parquet"))
        total_before = sum(p.stat().st_size for p in parts)
        merged = d / "_merged.parquet"

        # ROW_GROUP_SIZE per index type:
        #   number_index: 2000 (narrow street_lower ranges for bloom filter pushdown)
        #   geocoder (v4): 10000 (street-sorted, ~200 streets per group for tight
        #                  min/max pushdown on forward geocoding queries)
        #   geocoder (v1-v3): 25000 (h3-sorted, balanced spatial pushdown)
        #   other indexes: 25000
        d_str = str(d)
        if "number_index" in d_str:
            row_group_size = 2000
        elif "geocoder" in d_str and "addresses_v4" in d_str:
            row_group_size = 10000
        else:
            row_group_size = 25000
        # geocoder tiles have geometry, need geo_bbox for spatial pushdown
        geo_opt = ", GEOPARQUET_VERSION 'BOTH'" if "geocoder" in d_str else ""

        glob_path = str(d / "data_*.parquet")
        merge_con.sql(
            f"COPY (FROM read_parquet('{glob_path}')) "
            f"TO '{merged}' (FORMAT PARQUET, PARQUET_VERSION v2, "
            f"COMPRESSION ZSTD, COMPRESSION_LEVEL 6, "
            f"ROW_GROUP_SIZE {row_group_size}{geo_opt})"
        )

        for p in parts:
            p.unlink()
        merged.rename(d / "data_0.parquet")

        merged_size = (d / "data_0.parquet").stat().st_size
        log.info(
            "[MERGE] %s: %d files (%.1f MB) -> 1 (%.1f MB)",
            d.relative_to(scratch),
            len(parts),
            total_before / 1e6,
            merged_size / 1e6,
        )

    merge_con.close()
    elapsed = time.time() - t0
    log.info("[MERGE] Done in %.1fs (%d partitions merged)", elapsed, len(split_dirs))


def run_sql(con: duckdb.DuckDBPyConnection, sql_file: str, theme: str) -> None:
    """Execute a SQL script, splitting on semicolons, with progress logging."""
    sql = Path(sql_file).read_text()
    statements = sql.split(";")

    def _has_sql(raw: str) -> bool:
        """Check if a semicolon-delimited chunk contains actual SQL."""
        for line in raw.strip().split("\n"):
            s = line.strip()
            if s and not s.startswith("--") and not s.startswith(".print"):
                return True
        return False

    total_stmts = len([s for s in statements if _has_sql(s)])

    log.info("[SQL] Executing %s (%d statements)", sql_file, total_stmts)
    stmt_num = 0

    for stmt in statements:
        stmt = stmt.strip()
        if not stmt:
            continue

        # Strip .print directives and leading blank/comment lines
        lines = stmt.split("\n")
        sql_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith(".print"):
                msg = stripped.removeprefix(".print").strip().strip("'\"")
                log.info("[%s] %s", theme.upper(), msg)
            elif not sql_lines and (not stripped or stripped.startswith("--")):
                # Skip leading blank lines and comments (before any SQL)
                continue
            else:
                sql_lines.append(line)

        stmt = "\n".join(sql_lines).strip()
        if not stmt:
            continue

        stmt_num += 1
        stmt_preview = stmt.replace("\n", " ")[:120]
        is_select = stmt.strip().upper().startswith("SELECT")
        is_copy = stmt.strip().upper().startswith("COPY")
        is_create = stmt.strip().upper().startswith("CREATE")
        is_drop = stmt.strip().upper().startswith("DROP")

        if is_copy:
            log.info(
                "[%s] [%d/%d] COPY TO parquet... (mem=%s)",
                theme.upper(),
                stmt_num,
                total_stmts,
                _mem_gb(),
            )
        elif is_create and "TABLE" in stmt.upper():
            log.info(
                "[%s] [%d/%d] CREATE TABLE... (mem=%s)",
                theme.upper(),
                stmt_num,
                total_stmts,
                _mem_gb(),
            )
        elif is_drop:
            log.info(
                "[%s] [%d/%d] DROP TABLE (freeing memory)",
                theme.upper(),
                stmt_num,
                total_stmts,
            )

        t0 = time.time()
        try:
            result = con.sql(stmt)
            elapsed = time.time() - t0

            if result and is_select:
                rows = result.fetchall()
                for row in rows:
                    log.info("[%s]   → %s", theme.upper(), row)

            if elapsed > 5:
                log.info(
                    "[%s] [%d/%d] completed in %.1fs (mem=%s)",
                    theme.upper(),
                    stmt_num,
                    total_stmts,
                    elapsed,
                    _mem_gb(),
                )

        except Exception as e:
            elapsed = time.time() - t0
            log.error(
                "[%s] [%d/%d] FAILED after %.1fs: %s",
                theme.upper(),
                stmt_num,
                total_stmts,
                elapsed,
                e,
            )
            log.error("[%s] Statement: %s", theme.upper(), stmt_preview)
            raise


def _cleanup_disk() -> None:
    """Remove DuckDB temp files and log disk space."""
    temp_dir = Path("duckdb_temp.tmp")
    if temp_dir.exists():
        sz = sum(f.stat().st_size for f in temp_dir.rglob("*") if f.is_file())
        shutil.rmtree(str(temp_dir), ignore_errors=True)
        log.info("[CLEANUP] Removed duckdb_temp.tmp (%.1f GB)", sz / 1e9)

    try:
        total, used, free = shutil.disk_usage(".")
        log.info(
            "[CLEANUP] Disk: %.1f GB free / %.1f GB total", free / 1e9, total / 1e9
        )
    except Exception:
        pass


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Build H3 indices from Overture Maps")
    parser.add_argument("--release", default=os.environ.get("OVERTURE_RELEASE", ""))
    parser.add_argument(
        "--themes", default="addresses,places,transportation,base,buildings"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    _log_system_info()

    release = args.release or detect_latest_release()
    themes = [t.strip() for t in args.themes.split(",")]

    log.info("[PLAN] Release: %s", release)
    log.info("[PLAN] Themes:  %s (%d total)", ", ".join(themes), len(themes))
    log.info("[PLAN] SQL files: %s", [INDEX_BUILDERS.get(t, "MISSING") for t in themes])

    if args.dry_run:
        log.info("[PLAN] Dry run — exiting")
        return

    # Setup
    pipeline_t0 = time.time()
    con = get_duckdb()
    s3_base = configure_output(con)

    # Smoke-test the upload path before DuckDB does any heavy work.
    # Writes a tiny .txt to S3 so we fail fast on bad credentials,
    # missing s5cmd/aws-cli, or wrong bucket permissions.
    if s3_base:
        log.info("[PREFLIGHT] Testing upload to %s ...", s3_base)
        preflight_dir = Path("scratch/_preflight")
        preflight_dir.mkdir(parents=True, exist_ok=True)
        (preflight_dir / "_upload_test.txt").write_text(
            f"preflight check — release={release}\n"
        )
        try:
            s5cmd_upload(str(preflight_dir.resolve()), s3_base)
            log.info("[PREFLIGHT] Upload OK")
        except Exception as e:
            log.error("[PREFLIGHT] Upload FAILED: %s", e)
            log.error("[PREFLIGHT] Fix credentials or install s5cmd before continuing.")
            shutil.rmtree(str(preflight_dir), ignore_errors=True)
            sys.exit(1)
        shutil.rmtree(str(preflight_dir), ignore_errors=True)

    # Build each theme
    completed = []
    failed = []

    for i, theme in enumerate(themes, 1):
        sql_file = INDEX_BUILDERS.get(theme)
        if not sql_file or not Path(sql_file).exists():
            log.warning("[SKIP] No builder for '%s'", theme)
            failed.append(theme)
            continue

        log.info("=" * 60)
        log.info("[START] Theme %d/%d: %s", i, len(themes), theme)
        log.info("=" * 60)

        # Compute S3 destination and local scratch directory.
        # DuckDB writes to local disk (fast NVMe), then s5cmd uploads
        # in parallel. This is 5-10x faster than DuckDB writing to S3
        # directly via httpfs (17K+ files = 51K+ HTTP round-trips).
        if theme == "addresses_v4":
            s3_dest = f"{s3_base}/addresses-index/v4/release={release}"
        elif theme == "addresses":
            s3_dest = f"{s3_base}/{theme}-index/v2/release={release}"
        else:
            s3_dest = f"{s3_base}/{theme}-index/v1/release={release}/h3"

        # Local scratch: use ./scratch/<theme> (NVMe, not tmpfs)
        scratch_dir = Path("scratch") / theme
        if scratch_dir.exists():
            shutil.rmtree(str(scratch_dir))
        scratch_dir.mkdir(parents=True, exist_ok=True)
        local_out = str(scratch_dir.resolve())

        # If no S3 bucket, write to ./output/ directly
        if not s3_base:
            local_out = str(Path("output").resolve())
            Path(local_out).mkdir(parents=True, exist_ok=True)

        con.sql(f"SET VARIABLE overture_release = '{release}'")
        con.sql(f"SET VARIABLE output_dir = '{local_out}'")
        con.sql(
            f"SET VARIABLE overture_source = "
            f"'s3://overturemaps-us-west-2/release/{release}'"
        )

        log.info(
            "[%s] Source: s3://overturemaps-us-west-2/release/%s/",
            theme.upper(),
            release,
        )
        log.info("[%s] Local scratch: %s", theme.upper(), local_out)
        if s3_base:
            log.info("[%s] S3 dest: %s", theme.upper(), s3_dest)

        theme_t0 = time.time()
        try:
            # Phase 1: DuckDB writes to local disk
            run_sql(con, sql_file, theme)
            sql_elapsed = time.time() - theme_t0
            log.info(
                "[%s] SQL done in %.1f min (mem=%s)",
                theme.upper(),
                sql_elapsed / 60,
                _mem_gb(),
            )

            # Phase 2: Merge any split partition files into single files.
            # DuckDB creates data_0, data_1, ... when open file limit is
            # exceeded. Frontend needs exactly one file per partition.
            merge_split_partitions(local_out)

            # Phase 3: Upload to S3 in parallel
            if s3_base:
                s5cmd_upload(local_out, s3_dest)

            elapsed = time.time() - theme_t0
            log.info(
                "[DONE] %s complete in %.1f min (mem=%s)",
                theme,
                elapsed / 60,
                _mem_gb(),
            )
            completed.append(theme)
        except Exception as e:
            elapsed = time.time() - theme_t0
            log.error(
                "[FAIL] %s failed after %.1f min: %s",
                theme,
                elapsed / 60,
                e,
            )
            failed.append(theme)

        # Disk cleanup: remove scratch and DuckDB temp files
        if scratch_dir.exists():
            sz = sum(f.stat().st_size for f in scratch_dir.rglob("*") if f.is_file())
            shutil.rmtree(str(scratch_dir), ignore_errors=True)
            log.info("[CLEANUP] Removed scratch/%s (%.1f GB)", theme, sz / 1e9)
        _cleanup_disk()

    # Save state
    Path("state").mkdir(exist_ok=True)
    Path("state/last-release.txt").write_text(release + "\n")

    # Final summary
    total_elapsed = time.time() - pipeline_t0
    log.info("=" * 60)
    log.info("[SUMMARY] Release: %s", release)
    log.info("[SUMMARY] Total time: %.1f min", total_elapsed / 60)
    log.info(
        "[SUMMARY] Completed: %s (%d/%d)",
        ", ".join(completed) or "none",
        len(completed),
        len(themes),
    )
    if failed:
        log.error("[SUMMARY] Failed: %s", ", ".join(failed))
    log.info("[SUMMARY] Peak memory: %s", _mem_gb())
    log.info("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
