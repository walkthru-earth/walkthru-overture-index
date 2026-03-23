# Overture Maps Global Indices (H3-indexed)

[Overture Maps](https://overturemaps.org/) data aggregated to [H3](https://h3geo.org/) hexagonal cells. Five thematic indices at resolutions 1-10, updated automatically on each Overture release. All files are Parquet v2 with ZSTD compression, sorted by BIGINT `h3_index`.

| | |
|---|---|
| **Source** | [Overture Maps](https://overturemaps.org/) (2026-03-18.0+) |
| **Format** | Apache Parquet v2 (BIGINT h3_index, BYTE_STREAM_SPLIT + DELTA_BINARY_PACKED encodings) |
| **License** | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links) |
| **Code** | [walkthru-earth/walkthru-overture-index](https://github.com/walkthru-earth/walkthru-overture-index) |

## Quick Start

```sql
INSTALL h3 FROM community; LOAD h3;
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';

-- Transportation: road density by class
SELECT h3_index, segment_count, n_motorway, n_residential, n_cycleway,
       h3_cell_to_lat(h3_index) AS lat, h3_cell_to_lng(h3_index) AS lng
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/transportation-index/v1/release=2026-03-18.0/h3/h3_res=7/data.parquet')
WHERE segment_count > 100
ORDER BY segment_count DESC
LIMIT 20;

-- Places: commercial activity density
SELECT h3_index, place_count, avg_confidence,
       n_food_and_drink, n_shopping, n_education,
       h3_cell_to_lat(h3_index) AS lat, h3_cell_to_lng(h3_index) AS lng
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/places-index/v1/release=2026-03-18.0/h3/h3_res=7/data.parquet')
ORDER BY place_count DESC
LIMIT 20;

-- Buildings: residential vs commercial classification
SELECT h3_index, building_count, n_residential, n_commercial, n_industrial,
       avg_height_m, avg_floors,
       h3_cell_to_lat(h3_index) AS lat, h3_cell_to_lng(h3_index) AS lng
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/buildings-index/v1/release=2026-03-18.0/h3/h3_res=7/data.parquet')
WHERE building_count > 500
ORDER BY n_commercial DESC
LIMIT 20;

-- Addresses: reverse geocode (find nearest address to a point)
-- Step 1: use tile_index to find the right tile
SELECT country, h3_parent, address_count
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/addresses-index/v1/release=2026-03-18.0/tile_index.parquet')
WHERE country = 'NL' AND h3_parent = '841e933ffffffff';

-- Step 2: fetch that single tile (~1 MB) and search
SELECT full_address, street, number, city, postcode,
       ST_X(geometry) AS lon, ST_Y(geometry) AS lat
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/addresses-index/v1/release=2026-03-18.0/geocoder/country=NL/h3/841e933ffffffff.parquet')
WHERE full_address ILIKE '%keizersgracht%'
LIMIT 10;

-- Base environment: infrastructure + land use + water
SELECT h3_index, infra_count, landuse_count, water_count,
       n_lu_residential, n_lu_park, n_river, n_lake,
       h3_cell_to_lat(h3_index) AS lat, h3_cell_to_lng(h3_index) AS lng
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/base-index/v1/release=2026-03-18.0/h3/h3_res=7/data.parquet')
WHERE infra_count + landuse_count + water_count > 50
LIMIT 20;
```

## Files

```
walkthru-earth/indices/

  ── H3 Aggregate Indices (4 themes) ──────────────────────────

  transportation-index/v1/release={version}/h3/
    h3_res=1/data.parquet
    h3_res=2/data.parquet
    ...
    h3_res=10/data.parquet

  places-index/v1/release={version}/h3/
    h3_res=1/data.parquet  ...  h3_res=10/data.parquet

  buildings-index/v1/release={version}/h3/
    h3_res=1/data.parquet  ...  h3_res=10/data.parquet

  base-index/v1/release={version}/h3/
    h3_res=1/data.parquet  ...  h3_res=10/data.parquet

  ── Address Geocoder (cloud-native tile layout) ──────────────

  addresses-index/v1/release={version}/
    manifest.parquet                          ← per-country summary (39 rows)
    tile_index.parquet                        ← per-tile stats + bbox (~17K rows)
    postcode_index.parquet                    ← postcode → tile list
    region_index.parquet                      ← region → tile list + bbox
    city_index.parquet                        ← city → tile list
    geocoder/
      country=US/
        h3/
          842a101ffffffff.parquet              ← flat GeoParquet tile (~1 MB)
          842a107ffffffff.parquet
          ...                                 (~3,931 tiles for US)
      country=BR/
        h3/
          84a8101ffffffff.parquet
          ...                                 (~4,204 tiles for BR)
      country=DE/
        h3/
          841e111ffffffff.parquet
          ...                                 (~234 tiles for DE)
      ...                                     (39 countries, ~17,500 tiles total)
```

## Index Schemas

### Addresses — Geocoder Tiles (12 columns per tile)

| Column | Description |
|--------|-------------|
| `id` | Overture address ID |
| `geometry` | Point geometry (EPSG:4326, GeoParquet) |
| `country` | ISO 3166-1 alpha-2 country code |
| `postcode` | Postal code |
| `street` | Street name |
| `number` | House number |
| `unit` | Unit/apartment |
| `city` | City (country-aware extraction from address_levels) |
| `region` | State/province/region |
| `full_address` | Formatted address string (for FTS) |
| `h3_index` | H3 res 5 hex string (no h3 extension needed) |
| `h3_parent` | H3 res 4 hex string (= tile file name) |

### Addresses — Lookup Indexes

| File | Schema | Purpose |
|------|--------|---------|
| `manifest.parquet` | country, address_count, tile_count, bbox, overture_release | Per-country summary |
| `tile_index.parquet` | country, h3_parent, address_count, bbox, unique_postcodes, primary_region | Tile discovery for reverse geocoding |
| `postcode_index.parquet` | country, postcode, tiles[], addr_count | Postcode → tile mapping for forward geocoding |
| `region_index.parquet` | country, region, tiles[], addr_count, bbox | Region → tile mapping |
| `city_index.parquet` | country, region, city, tiles[], addr_count | City → tile mapping |

### Places (31 columns)

| Column | Description |
|--------|-------------|
| `h3_index` | H3 cell ID (BIGINT) |
| `place_count` | Total POIs |
| `avg_confidence` | Weighted average confidence (0-1) |
| `n_food_and_drink` .. `n_geographic_entities` | 13 top categories (from `taxonomy.hierarchy[1]`) |
| `n_restaurant` .. `n_school` | 15 subcategories (from `categories.primary`) |

### Transportation (27 columns)

| Column | Description |
|--------|-------------|
| `h3_index` | H3 cell ID (BIGINT) |
| `segment_count` | Total road/rail/water segments |
| `n_road`, `n_rail`, `n_water` | By subtype |
| `n_motorway` .. `n_bridleway`, `n_class_unknown` | 17 road classes |
| `n_bridge`, `n_tunnel`, `n_link` | Road flags |
| `n_paved`, `n_unpaved` | Surface type |

### Base Environment (42 columns)

| Column | Description |
|--------|-------------|
| `h3_index` | H3 cell ID (BIGINT) |
| `infra_count` | Total infrastructure features |
| `n_power` .. `n_communication` | 14 infrastructure subtypes |
| `landuse_count` | Total land use features |
| `n_lu_agriculture` .. `n_lu_protected` | 15 land use subtypes |
| `water_count` | Total water features |
| `n_river` .. `n_spring` | 8 water subtypes |
| `n_salt_water`, `n_intermittent` | Water properties |

### Buildings (42 columns)

| Column | Description |
|--------|-------------|
| `h3_index` | H3 cell ID (BIGINT) |
| `building_count` | Total buildings |
| `n_residential` .. `n_outbuilding` | 13 subtypes |
| `n_house` .. `n_hangar` | 20 detailed classes |
| `n_has_parts` | Complex buildings |
| `n_with_height`, `avg_height_m`, `max_height_m` | Height stats |
| `n_with_floors`, `avg_floors`, `max_floors` | Floor stats |

## Cross-Index Joins

All indices share the same BIGINT `h3_index` at the same resolutions, enabling powerful cross-index analysis:

```sql
-- Urban liveability: places + buildings + transportation + addresses
SELECT
    p.h3_index,
    h3_cell_to_lat(p.h3_index) AS lat,
    h3_cell_to_lng(p.h3_index) AS lng,
    p.place_count,
    b.building_count,
    b.n_residential,
    t.segment_count,
    t.n_cycleway + t.n_footway + t.n_pedestrian AS walkable_segments,
    a.address_count
FROM read_parquet('.../places-index/v1/release=2026-03-18.0/h3/h3_res=7/data.parquet') p
JOIN read_parquet('.../buildings-index/v1/release=2026-03-18.0/h3/h3_res=7/data.parquet') b ON p.h3_index = b.h3_index
JOIN read_parquet('.../transportation-index/v1/release=2026-03-18.0/h3/h3_res=7/data.parquet') t ON p.h3_index = t.h3_index
JOIN read_parquet('.../addresses-index/v1/release=2026-03-18.0/h3/h3_res=7/data.parquet') a ON p.h3_index = a.h3_index
WHERE p.place_count > 50
ORDER BY walkable_segments DESC
LIMIT 20;
```

## Source

> Overture Maps Foundation (2026). Overture Maps Data. [overturemaps.org](https://overturemaps.org/). Licensed under [ODbL](https://opendatacommons.org/licenses/odbl/) and [CDLA Permissive 2.0](https://cdla.dev/permissive-2-0/).

## License

This dataset is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links). Source data by [Overture Maps Foundation](https://overturemaps.org/) under ODbL/CDLA Permissive 2.0.
