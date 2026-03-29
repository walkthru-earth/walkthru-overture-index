-- ============================================================
-- Walkthru Cloud-Native Address Geocoder v3 — DuckDB 1.5
--
-- Source:  Overture Maps addresses (direct from S3)
-- Output: Three-level partitioned GeoParquet geocoder tiles
--         optimized for both reverse AND forward geocoding
--         in DuckDB-WASM via HTTP range requests
--
-- Partitioning strategy (v3, three levels):
--   Level 1: country (2-letter code)
--   Level 2: region (state/prefecture OR H3 res 2 proxy)
--   Level 3: h3_parent (H3 res 4, ~1,770 km²)
--
--   Region source per country:
--     address_levels[1] for countries with ASCII-safe state codes:
--       US(50), BR(27), AU(9), CA(13), CO(32), DE(15), GL(78),
--       HK(3), UY(19)
--     H3 res 2 hex for all others (non-ASCII chars in state names,
--     or no state-level admin data):
--       JP (kanji), TW (CJK), MX/CL (accents), IT (slashes),
--       DK/EE/SK/SI/LV/FO (diacritics),
--       FR, ES, NL, BE, PT, FI, NO, CH, CZ, RS, AT, NZ, PL,
--       HR, LT, LU, IS, SG, LI, GB
--
--   Benefits over v2 (country > h3_parent):
--     - All indexes partitioned by (country, region) for smaller files
--     - TX street_index ~0.5 MB vs full US ~5 MB (10x smaller)
--     - Reverse geocoding scopes tile lookup to region
--     - Better cache locality for region-specific usage
--     - Same total tile files (~17,500), reorganized into subdirectories
--
-- Output layout (DuckDB writes locally, s5cmd uploads to S3):
--   geocoder/country=XX/region=YY/h3_parent=ZZZ/data_0.parquet
--   manifest.parquet                                         (per-country, global)
--   tile_index.parquet                                       (per-tile with region, global)
--   region_index.parquet                                     (region -> tiles, global)
--   city_index/country=XX/region=YY/data_0.parquet           (per-region)
--   postcode_index/country=XX/region=YY/data_0.parquet       (per-region)
--   street_index/country=XX/region=YY/data_0.parquet         (per-region)
--   number_index/country=XX/region=YY/data_0.parquet         (per-region)
--
-- Cloud-native query flows:
--
--   Reverse geocode (point -> address):
--     1. h3_latlng_to_cell(lat, lon, 5) -> h3_index
--     2. h3_cell_to_parent(h3_index, 4) -> h3_parent
--     3. Lookup _tile_index (cached) -> get country + region for h3_parent
--     4. Fetch geocoder/country=XX/region=YY/h3_parent=ZZZ/data_0.parquet
--
--   Forward geocode (text -> point):
--     1. User selects country -> load region_index (global, ~95 KB)
--     2. User selects region -> load region-scoped indexes:
--        city_index/country=XX/region=YY/data_0.parquet
--        street_index/country=XX/region=YY/data_0.parquet
--        postcode_index/country=XX/region=YY/data_0.parquet
--     3. Autocomplete narrows to tiles within that region
--     4. Fetch geocoder/country=XX/region=YY/h3_parent=ZZZ/data_0.parquet
--     5. number_index queried on-demand per region (HTTP range requests)
-- ============================================================

-- ----------------------------------------------------------
-- Step 1: Enrich Overture addresses into geocoder schema
-- ----------------------------------------------------------
-- Reads source once from S3 (470M rows, ~22 GB).
-- Region is ALWAYS populated: state-level admin where available,
-- H3 res 2 hex string as geographic proxy elsewhere.

.print '>>> Step 1: Enriching 470M Overture addresses...'

CREATE OR REPLACE TABLE _enriched AS
WITH raw AS (
    SELECT
        id,
        ST_SetCRS(geometry, 'EPSG:4326') AS geometry,
        country,
        postcode,
        street,
        CASE WHEN country = 'JP' AND number LIKE '%-%'
            THEN split_part(number, '-', 1)
            ELSE number
        END AS number,
        unit,
        postal_city,
        address_levels,
        h3_latlng_to_cell(ST_Y(geometry), ST_X(geometry), 5) AS h3_index
    FROM read_parquet(
        getvariable('overture_source') || '/theme=addresses/type=address/*',
        hive_partitioning=0
    )
    WHERE geometry IS NOT NULL
      AND country IS NOT NULL
      AND (street IS NOT NULL OR number IS NOT NULL)
),
with_city_region AS (
    SELECT
        id, geometry, country, postcode, street, number, unit,
        h3_index,
        h3_h3_to_string(h3_cell_to_parent(h3_index, 4)) AS h3_parent,
        -- City: finest populated administrative level
        -- Overture address_levels ordered broadest->finest:
        --   [1]=region/state  [2]=district/municipality  [3]=city/village
        -- Depth varies WITHIN a country (OvertureMaps/data#509)
        COALESCE(
            CASE WHEN len(address_levels) >= 3 THEN address_levels[3].value END,
            CASE WHEN len(address_levels) >= 2 THEN address_levels[2].value END,
            CASE WHEN len(address_levels) >= 1 THEN address_levels[1].value END,
            postal_city
        ) AS city,
        -- Region: always populated for v3 partitioning
        -- S3 paths require ASCII-safe values (DuckDB httpfs does not
        -- URL-encode non-ASCII characters in Hive partition paths).
        -- Countries with ASCII-safe state codes in address_levels[1]:
        --   US(50), BR(27), AU(9), CA(13), CO(32), DE(15), GL(78),
        --   HK(3), UY(19) — use the name directly
        -- All other countries (JP kanji, MX/CL accents, IT slashes, etc.):
        --   Use H3 res 2 hex as geographic proxy (~15-25 cells per country)
        CASE
            WHEN len(address_levels) >= 2
                 AND country IN ('US','BR','AU','CA','CO','DE','GL','HK','UY')
            THEN address_levels[1].value
            ELSE h3_h3_to_string(h3_cell_to_parent(h3_latlng_to_cell(
                ST_Y(geometry), ST_X(geometry), 4), 2))
        END AS region
    FROM raw
)
SELECT
    id, geometry, country, postcode, street, number, unit,
    city, region,
    CONCAT_WS(', ',
        NULLIF(TRIM(
            CASE
                WHEN country IN ('US','CA','AU','NZ','BR','MX','CL','CO','UY','SG','HK','TW')
                    THEN COALESCE(number, '') || ' ' || COALESCE(street, '')
                ELSE COALESCE(street, '') || ' ' || COALESCE(number, '')
            END
        ), ''),
        city,
        region,
        postcode
    ) AS full_address,
    h3_index,
    h3_parent
FROM with_city_region;

SELECT
    count(*) AS total_enriched,
    count(DISTINCT country) AS countries,
    count(DISTINCT region) AS distinct_regions,
    count(DISTINCT h3_parent) AS h3_parent_tiles,
    count(DISTINCT h3_index) AS h3_res5_cells,
    count(city) AS has_city,
    count(region) AS has_region,
    count(full_address) AS has_full_address,
    ST_CRS(first(geometry)) AS crs
FROM _enriched;

-- Validate: every row must have a region (critical for partitioning)
SELECT count(*) AS rows_missing_region
FROM _enriched
WHERE region IS NULL;

-- ----------------------------------------------------------
-- Step 2: Export geocoder tiles (3-level partitioning)
-- ----------------------------------------------------------
-- country > region > h3_parent
-- ROW_GROUP_SIZE 25000 (~2.5 MB per group) for bloom filter pushdown.
-- DuckDB auto-writes bloom filters on dict-encoded columns.

.print '>>> Step 2: Exporting geocoder tiles (country > region > h3_parent)...'

COPY (
    SELECT
        id, geometry, country, postcode, street, number, unit,
        city, region, full_address, h3_index, h3_parent
    FROM _enriched
    ORDER BY country, region, h3_parent, h3_index
) TO (getvariable('output_dir') || '/geocoder/')
(FORMAT PARQUET,
 PARTITION_BY (country, region, h3_parent),
 PARQUET_VERSION v2,
 COMPRESSION ZSTD,
 COMPRESSION_LEVEL 6,
 ROW_GROUP_SIZE 25000,
 GEOPARQUET_VERSION 'BOTH',
 OVERWRITE);

-- ----------------------------------------------------------
-- Step 3: Build combined aggregation (single scan of _enriched)
-- ----------------------------------------------------------

.print '>>> Step 3: Building combined aggregation (single scan)...'

CREATE OR REPLACE TABLE _agg AS
SELECT
    country,
    region,
    h3_parent,
    postcode,
    city,
    count(*)::INTEGER AS cnt,
    min(ST_X(geometry)) AS min_lon,
    max(ST_X(geometry)) AS max_lon,
    min(ST_Y(geometry)) AS min_lat,
    max(ST_Y(geometry)) AS max_lat
FROM _enriched
GROUP BY country, region, h3_parent, postcode, city;

SELECT count(*) AS agg_rows FROM _agg;

-- Build street aggregation before dropping _enriched.

.print '>>> Step 3b: Building street aggregation...'

CREATE OR REPLACE TABLE _street_agg AS
SELECT
    country,
    region,
    h3_parent,
    lower(street) AS street_lower,
    city,
    count(*)::INTEGER AS cnt,
    avg(ST_X(geometry)) AS centroid_lon,
    avg(ST_Y(geometry)) AS centroid_lat
FROM _enriched
WHERE street IS NOT NULL
GROUP BY country, region, h3_parent, lower(street), city;

SELECT count(*) AS street_agg_rows FROM _street_agg;

-- Build number aggregation before dropping _enriched.

.print '>>> Step 3c: Building number aggregation...'

CREATE OR REPLACE TABLE _number_agg AS
SELECT
    country,
    region,
    lower(street) AS street_lower,
    list(DISTINCT number ORDER BY number) AS numbers
FROM _enriched
WHERE street IS NOT NULL AND number IS NOT NULL AND number != ''
GROUP BY country, region, lower(street);

SELECT count(*) AS number_agg_rows FROM _number_agg;

-- Free the big table immediately (~133 GB)
DROP TABLE _enriched;

.print '>>> _enriched dropped, building indexes from _agg + _street_agg...'

-- ----------------------------------------------------------
-- Step 4: Build tile statistics (from _agg)
-- ----------------------------------------------------------

.print '>>> Step 4: Computing tile statistics...'

CREATE OR REPLACE TABLE _tile_stats AS
SELECT
    country,
    region,
    h3_parent,
    sum(cnt)::INTEGER AS address_count,
    min(min_lon) AS bbox_min_lon,
    max(max_lon) AS bbox_max_lon,
    min(min_lat) AS bbox_min_lat,
    max(max_lat) AS bbox_max_lat,
    count(DISTINCT postcode) FILTER (postcode IS NOT NULL)::INTEGER AS unique_postcodes,
    count(DISTINCT city) FILTER (city IS NOT NULL)::INTEGER AS unique_cities
FROM _agg
GROUP BY country, region, h3_parent;

SELECT
    count(*) AS total_tiles,
    sum(address_count) AS total_addresses,
    avg(address_count)::INTEGER AS avg_per_tile,
    max(address_count) AS max_per_tile,
    min(address_count) AS min_per_tile,
    count(DISTINCT country) AS countries,
    count(DISTINCT region) AS regions
FROM _tile_stats;

-- ----------------------------------------------------------
-- Step 5: Export manifest (per-country summary)
-- ----------------------------------------------------------

.print '>>> Step 5: Exporting manifest...'

COPY (
    SELECT
        country,
        sum(address_count)::INTEGER AS address_count,
        count(*)::INTEGER AS tile_count,
        count(DISTINCT region)::INTEGER AS region_count,
        min(bbox_min_lon) AS bbox_min_lon,
        max(bbox_max_lon) AS bbox_max_lon,
        min(bbox_min_lat) AS bbox_min_lat,
        max(bbox_max_lat) AS bbox_max_lat,
        4 AS h3_parent_resolution,
        5 AS h3_index_resolution,
        getvariable('overture_release') AS overture_release
    FROM _tile_stats
    GROUP BY country
    ORDER BY address_count DESC
) TO (getvariable('output_dir') || '/manifest.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

-- ----------------------------------------------------------
-- Step 6: Export tile index (per-tile stats with region)
-- ----------------------------------------------------------
-- Global file, cached in WASM as _tile_index.
-- Frontend uses it for: h3_parent -> (country, region) lookup
-- when constructing 3-level tile URLs.

.print '>>> Step 6: Exporting tile index...'

COPY (
    SELECT * FROM _tile_stats
    ORDER BY country, region, h3_parent
) TO (getvariable('output_dir') || '/tile_index.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

DROP TABLE _tile_stats;

-- ----------------------------------------------------------
-- Step 7: Export postcode index — partitioned by (country, region)
-- ----------------------------------------------------------

.print '>>> Step 7: Building postcode index (per-region)...'

COPY (
    SELECT
        country,
        region,
        postcode,
        list(DISTINCT h3_parent ORDER BY h3_parent) AS tiles,
        sum(cnt)::INTEGER AS addr_count,
        ROUND((min(min_lon) + max(max_lon)) / 2 * 1e6)::INTEGER AS centroid_lon_e6,
        ROUND((min(min_lat) + max(max_lat)) / 2 * 1e6)::INTEGER AS centroid_lat_e6
    FROM _agg
    WHERE postcode IS NOT NULL AND postcode != ''
    GROUP BY country, region, postcode
    ORDER BY country, region, postcode
) TO (getvariable('output_dir') || '/postcode_index/')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD,
 PARTITION_BY (country, region), OVERWRITE);

-- ----------------------------------------------------------
-- Step 8: Export region index (region -> tiles mapping)
-- ----------------------------------------------------------
-- Global file. Frontend loads this after country selection
-- to show available regions and their bounding boxes.

.print '>>> Step 8: Building region index...'

COPY (
    SELECT
        country,
        region,
        list(DISTINCT h3_parent ORDER BY h3_parent) AS tiles,
        sum(cnt)::INTEGER AS addr_count,
        min(min_lon) AS bbox_min_lon,
        max(max_lon) AS bbox_max_lon,
        min(min_lat) AS bbox_min_lat,
        max(max_lat) AS bbox_max_lat
    FROM _agg
    GROUP BY country, region
    ORDER BY country, region
) TO (getvariable('output_dir') || '/region_index.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

-- ----------------------------------------------------------
-- Step 9: Export city index — partitioned by (country, region)
-- ----------------------------------------------------------

.print '>>> Step 9: Building city index (per-region)...'

COPY (
    SELECT
        country,
        region,
        city,
        list(DISTINCT h3_parent ORDER BY h3_parent) AS tiles,
        sum(cnt)::INTEGER AS addr_count,
        ROUND(min(min_lon) * 1e6)::INTEGER AS bbox_min_lon_e6,
        ROUND(max(max_lon) * 1e6)::INTEGER AS bbox_max_lon_e6,
        ROUND(min(min_lat) * 1e6)::INTEGER AS bbox_min_lat_e6,
        ROUND(max(max_lat) * 1e6)::INTEGER AS bbox_max_lat_e6
    FROM _agg
    WHERE city IS NOT NULL
    GROUP BY country, region, city
    ORDER BY country, region, city
) TO (getvariable('output_dir') || '/city_index/')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD,
 PARTITION_BY (country, region), OVERWRITE);

-- ----------------------------------------------------------
-- Step 10: Export street index — partitioned by (country, region)
-- ----------------------------------------------------------

.print '>>> Step 10: Building street index (per-region)...'

COPY (
    SELECT
        country,
        region,
        street_lower,
        list(DISTINCT h3_parent ORDER BY h3_parent) AS tiles,
        sum(cnt)::INTEGER AS addr_count,
        arg_max(city, cnt) AS primary_city,
        ROUND(sum(centroid_lon * cnt) / sum(cnt) * 1e6)::INTEGER AS centroid_lon_e6,
        ROUND(sum(centroid_lat * cnt) / sum(cnt) * 1e6)::INTEGER AS centroid_lat_e6
    FROM _street_agg
    GROUP BY country, region, street_lower
    ORDER BY country, region, street_lower
) TO (getvariable('output_dir') || '/street_index/')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD,
 PARTITION_BY (country, region), OVERWRITE);

DROP TABLE _street_agg;

-- ----------------------------------------------------------
-- Step 11: Export number index — partitioned by (country, region)
-- ----------------------------------------------------------

.print '>>> Step 11: Building number index (per-region)...'

COPY (
    SELECT country, region, street_lower, numbers
    FROM _number_agg
    ORDER BY country, region, street_lower
) TO (getvariable('output_dir') || '/number_index/')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD,
 ROW_GROUP_SIZE 2000,
 PARTITION_BY (country, region), OVERWRITE);

DROP TABLE _number_agg;

-- ----------------------------------------------------------
-- Step 12: Cleanup
-- ----------------------------------------------------------

.print '>>> Step 12: Cleanup...'
DROP TABLE _agg;

.print '>>> GEOCODER v3 BUILD COMPLETE'
