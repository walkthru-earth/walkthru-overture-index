# CLAUDE.md

## Project overview

Builds H3-indexed global indices from Overture Maps data using DuckDB 1.5. Reads from public S3, writes to Source Cooperative S3. Runs on Hetzner Cloud via GitHub Actions.

## Hard-won lessons

### SQL parser

- **Never put semicolons in SQL comments.** The SQL runner splits on `;` — a semicolon inside `-- dense urban; sparse` breaks the comment into two chunks, the second becoming invalid SQL.
- **The parser strips leading blank lines AND comment lines** before deciding if a chunk is executable. Both must be skipped together — blank lines between comment blocks used to make the parser think subsequent comments were SQL.
- **`.print` directives** are extracted and logged, not sent to DuckDB.

### DuckDB TABLE MACROs

- **Use `query_table(src_tbl)`** not `FROM src_tbl`. DuckDB resolves bare identifiers at macro definition time, causing "Table with name src_tbl does not exist". `query_table()` defers resolution to call time.
- Macro calls work with both quoted (`'h3_res10'`) and unquoted (`h3_res10`) table names.

### DuckDB S3 authentication

- **DuckDB auto-detects `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` from env vars** and applies them globally — even to public buckets that need anonymous access.
- Fix: create an explicit secret with **empty `KEY_ID ''` and `SECRET ''`** scoped to the public bucket. `PROVIDER CREDENTIAL_CHAIN` with `CHAIN ''` does NOT work.
- Each secret needs a `SCOPE` matching the S3 URL prefix. DuckDB picks the most specific matching scope.

```sql
-- Anonymous for public Overture bucket
CREATE SECRET overture_s3 (TYPE S3, KEY_ID '', SECRET '', REGION 'us-west-2', SCOPE 's3://overturemaps-us-west-2');

-- Authenticated for output bucket
CREATE SECRET output_s3 (TYPE S3, KEY_ID '...', SECRET '...', REGION 'us-west-2', URL_STYLE 'path', SCOPE 's3://us-west-2.opendata.source.coop');
```

### DuckDB memory management

- **Always set `memory_limit`** to ~75% of system RAM. Without it, DuckDB uses all available memory, starving the OS of RAM for networking/DNS — causing DNS resolution failures mid-query (not OOM kills).
- **Set `temp_directory`** so DuckDB can spill to disk when hitting the memory limit.
- `_mem_gb()` reports peak RSS (high-water mark) — it never decreases even after `DROP TABLE`. DuckDB does free memory internally.

### Overture Maps schema (2026-03-18.0)

- **Places `categories.primary`** contains **leaf-level** categories (`restaurant`, `cafe`, `hotel`), NOT top-level ones. Top-level categories (`food_and_drink`, `shopping`) come from **`taxonomy.hierarchy[1]`** (1-indexed array).
- **Addresses** column is `postcode`, not `postal_code`.
- **`road_flags`** is `STRUCT("values" VARCHAR[], "between" DOUBLE[])[]` — use `road_flags::VARCHAR LIKE '%is_bridge%'` to check flags, not `list_contains()`.
- **`road_surface`** access: `road_surface[1].value` (1-indexed).
- **Base theme subtypes change between releases.** Always validate with `SELECT subtype, count(*) ... GROUP BY ALL` before writing SQL. Several subtypes we assumed (military, religious, medical for land_use) don't exist.

### Hetzner Cloud runners

- **Set `HOME: /root`** as a job-level env var. Self-hosted runners run as root with no HOME set — DuckDB and uv both need it.
- **Use `astral-sh/setup-uv@v7`**, not `curl | sh` for uv installation.
- **Name servers** with a pattern: `overture-{server_type}-{run_id}` for easy identification in the Hetzner dashboard.
- The `PERSONAL_ACCESS_TOKEN` must be a valid GitHub PAT (not a Hetzner token).

### Pipeline design

- **Process themes lightest-first:** addresses → places → transportation → base → buildings. Partial results are available faster, and if the heaviest theme (buildings, 2.3B rows) fails, the others already succeeded.
- **Each resolution writes to S3 immediately**, then the source table is dropped. This is progressive — partial results are available even if the pipeline crashes mid-theme.
- **`COPY TO` overwrites** existing S3 files — safe to re-run the same release.
- **detect-release.yml** must pass all themes, not just one. It was hardcoded to `themes="transportation"` initially.

## Commands

```bash
uv sync                                    # Install deps
uv run python main.py --dry-run            # Preview plan
uv run python main.py --themes addresses   # Run one theme
uv run ruff check --fix . && uv run ruff format .  # Lint
```

## Do not

- Do not add semicolons inside SQL comments
- Do not use `FROM src_tbl` in TABLE MACROs — use `FROM query_table(src_tbl)`
- Do not assume Overture column names — always `DESCRIBE` or sample first
- Do not run without `memory_limit` on machines where AWS env vars are set
- Do not skip schema validation when Overture releases a new version
