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

-- Addresses: urbanization proxy
SELECT h3_index, address_count, unique_postcodes,
       h3_cell_to_lat(h3_index) AS lat, h3_cell_to_lng(h3_index) AS lng
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/indices/addresses-index/v1/release=2026-03-18.0/h3/h3_res=7/data.parquet')
ORDER BY address_count DESC
LIMIT 20;

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
  transportation-index/v1/release={version}/h3/
    h3_res=1/ .. h3_res=10/  data.parquet
  places-index/v1/release={version}/h3/
    h3_res=1/ .. h3_res=10/  data.parquet
  buildings-index/v1/release={version}/h3/
    h3_res=1/ .. h3_res=10/  data.parquet
  addresses-index/v1/release={version}/h3/
    h3_res=1/ .. h3_res=10/  data.parquet
  base-index/v1/release={version}/h3/
    h3_res=1/ .. h3_res=10/  data.parquet
```

## Index Schemas

### Transportation (27 columns)

| Column | Description |
|--------|-------------|
| `h3_index` | H3 cell ID (BIGINT) |
| `segment_count` | Total road/rail/water segments |
| `n_road`, `n_rail`, `n_water` | By subtype |
| `n_motorway` .. `n_bridleway`, `n_class_unknown` | 18 road classes |
| `n_bridge`, `n_tunnel`, `n_link` | Road flags |
| `n_paved`, `n_unpaved` | Surface type |

### Places (30 columns)

| Column | Description |
|--------|-------------|
| `h3_index` | H3 cell ID (BIGINT) |
| `place_count` | Total POIs |
| `avg_confidence` | Weighted average confidence (0-1) |
| `n_food_and_drink` .. `n_geographic_entities` | 13 top categories |
| `n_restaurant` .. `n_school` | 15 subcategories |

### Buildings (40 columns)

| Column | Description |
|--------|-------------|
| `h3_index` | H3 cell ID (BIGINT) |
| `building_count` | Total buildings |
| `n_residential` .. `n_outbuilding` | 13 subtypes |
| `n_house` .. `n_hangar` | 20 detailed classes |
| `n_has_parts` | Complex buildings |
| `avg_height_m`, `max_height_m` | Height stats |
| `avg_floors`, `max_floors` | Floor stats |

### Addresses (3 columns)

| Column | Description |
|--------|-------------|
| `h3_index` | H3 cell ID (BIGINT) |
| `address_count` | Total addresses |
| `unique_postcodes` | Distinct postal codes |

### Base Environment (41 columns)

| Column | Description |
|--------|-------------|
| `h3_index` | H3 cell ID (BIGINT) |
| `infra_count` | Total infrastructure features |
| `n_power` .. `n_waste_mgmt` | 13 infrastructure subtypes |
| `landuse_count` | Total land use features |
| `n_lu_residential` .. `n_lu_resource` | 16 land use subtypes |
| `water_count` | Total water features |
| `n_river` .. `n_spring` | 8 water subtypes |
| `n_salt_water`, `n_intermittent` | Water properties |

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
