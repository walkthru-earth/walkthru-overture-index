# walkthru-overture-index

H3-indexed global indices from [Overture Maps](https://overturemaps.org/) — transportation, places, buildings, addresses, and base environment.

Part of the [walkthru-earth](https://github.com/walkthru-earth) index family.

## What it does

Reads Overture Maps GeoParquet directly from S3 via DuckDB 1.5, builds H3-indexed indices at resolutions 1-10, and writes Parquet v2 output to S3. Automatically detects new Overture releases via their STAC catalog.

### Indices

| Index | Source | Rows | Key metrics |
|-------|--------|------|-------------|
| **Transportation** | segments | ~343M | Road class breakdown (motorway/primary/residential/...), bridges, tunnels, surface type, rail/water |
| **Places** | places | ~73M | 13 top categories (food, shopping, health, education...), 15 subcategories, confidence |
| **Buildings** | buildings | ~2.3B | 13 subtypes (residential/commercial/industrial/...), 20 classes, height/floor stats |
| **Addresses** | addresses | ~200M+ | Address count, unique postcodes per cell |
| **Base** | infrastructure + land_use + water | ~100M+ | Power/telecom/transit infrastructure, 16 land use types, water bodies |

### Complements existing indices

- **Buildings here** adds building USE classification (residential vs commercial vs industrial) — complements [walkthru-building-index](https://github.com/walkthru-earth/walkthru-building-index) which has height/footprint/volume metrics from Global Building Atlas
- **Places here** replaces the standalone places index with the same data but updated automatically on each Overture release

## Setup

```bash
uv sync
```

## Usage

```bash
# Auto-detect latest Overture release, build all 5 indices
uv run python main.py

# Specific release
uv run python main.py --release 2026-03-18.0

# Specific themes
uv run python main.py --themes transportation,places

# Preview without processing
uv run python main.py --dry-run
```

## Output

```
{output_base}/{theme}-index/v1/release={release}/h3/
  h3_res=1/  data.parquet
  h3_res=2/  ...
  ...
  h3_res=10/ data.parquet
```

All files: Parquet v2, ZSTD compression, sorted by `h3_index` (BIGINT), no geometry stored.

## Automation

Two GitHub Actions workflows:

1. **detect-release.yml** — Polls Overture STAC catalog daily. On new release, triggers the build.
2. **build-index.yml** — Spins up a Hetzner Cloud `ccx43` (16 vCPU, 64 GB RAM), runs all 5 indices sequentially (~3-4 hours), tears down. Cost: ~€0.64 per run.

### Secrets needed

| Secret | Purpose |
|--------|---------|
| `PERSONAL_ACCESS_TOKEN` | GitHub fine-grained PAT (for Hetzner runner registration) |
| `HCLOUD_TOKEN` | Hetzner Cloud API token |
| `S3_BUCKET` | Output S3 bucket |
| `S3_PREFIX` | Key prefix (e.g. `walkthru-earth/indices`) |
| `AWS_ACCESS_KEY_ID` | S3 write credentials |
| `AWS_SECRET_ACCESS_KEY` | S3 write credentials |
| `AWS_DEFAULT_REGION` | S3 region |

## Source

> Overture Maps Foundation (2026). Overture Maps Data. [overturemaps.org](https://overturemaps.org/). Licensed under [ODbL](https://opendatacommons.org/licenses/odbl/) and [CDLA Permissive 2.0](https://cdla.dev/permissive-2-0/).

## License

This project is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://github.com/walkthru-earth). See [LICENSE](LICENSE) for details. Source data by [Overture Maps Foundation](https://overturemaps.org/) under ODbL/CDLA Permissive 2.0.

Contact: [hi@walkthru.earth](mailto:hi@walkthru.earth)
