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
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import urlopen

import duckdb

from geocoder import export_and_upload

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
    """Configure S3 output credentials. Returns output base path."""
    if not S3_BUCKET:
        log.info("[OUTPUT] No S3_BUCKET — writing to ./output/")
        Path("output").mkdir(parents=True, exist_ok=True)
        return "output"

    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    if aws_key and aws_secret:
        con.sql(f"""
            CREATE OR REPLACE SECRET output_s3 (
                TYPE S3,
                KEY_ID '{aws_key}',
                SECRET '{aws_secret}',
                REGION '{AWS_REGION}',
                URL_STYLE 'path',
                SCOPE 's3://{S3_BUCKET}'
            )
        """)
        log.info("[OUTPUT] S3 secret configured for s3://%s/", S3_BUCKET)
    else:
        log.warning("[OUTPUT] S3_BUCKET set but AWS credentials missing!")

    parts = [S3_BUCKET]
    if S3_PREFIX:
        parts.append(S3_PREFIX)
    base = "/".join(parts)
    log.info("[OUTPUT] Base path: s3://%s/", base)
    return f"s3://{base}"


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
    output_base = configure_output(con)

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

        # Set variables accessible in SQL via getvariable()
        # Addresses uses a flat geocoder layout (no /h3 prefix).
        # Other themes keep the /h3 prefix for backward compatibility.
        if theme == "addresses":
            out_dir = f"{output_base}/{theme}-index/v1/release={release}"
        else:
            out_dir = f"{output_base}/{theme}-index/v1/release={release}/h3"
        con.sql(f"SET VARIABLE overture_release = '{release}'")
        con.sql(f"SET VARIABLE output_dir = '{out_dir}'")
        con.sql(
            f"SET VARIABLE overture_source = "
            f"'s3://overturemaps-us-west-2/release/{release}'"
        )

        # Addresses theme writes tiles to local scratch dir, then
        # flattens h3_parent dirs and uploads to S3.
        scratch_dir = None
        if theme == "addresses":
            scratch_dir = tempfile.mkdtemp(prefix="geocoder_")
            con.sql(f"SET VARIABLE scratch_dir = '{scratch_dir}'")
            log.info("[%s] Scratch dir: %s", theme.upper(), scratch_dir)

        log.info(
            "[%s] Source: s3://overturemaps-us-west-2/release/%s/",
            theme.upper(),
            release,
        )
        log.info("[%s] Output: %s/", theme.upper(), out_dir)

        theme_t0 = time.time()
        try:
            run_sql(con, sql_file, theme)

            # Post-process: flatten partition dirs and upload tiles
            if scratch_dir:
                # Parse S3 bucket/prefix from out_dir for boto3
                # out_dir = "s3://bucket/prefix/addresses-index/..."
                s3_path = out_dir.removeprefix("s3://")
                bucket = s3_path.split("/")[0]
                prefix = "/".join(s3_path.split("/")[1:])
                export_and_upload(scratch_dir, bucket, prefix)

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
            if scratch_dir:
                shutil.rmtree(scratch_dir, ignore_errors=True)

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
