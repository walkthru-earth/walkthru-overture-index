-- ============================================================
-- Walkthru Cloud-Native Address Geocoder — DuckDB 1.5
--
-- Source:  Overture Maps addresses (direct from S3)
-- Output: Country-partitioned geocoder tiles optimized for
--         HTTP range requests + DuckDB-WASM
--
-- Architecture:
--   Overture 470M addresses → enriched + H3-indexed Parquet
--   tiles partitioned by country, sorted by (lat, lon) so
--   row-group min/max stats enable spatial pushdown via
--   standard Parquet range requests. No spatial extension
--   needed at query time (lat/lon doubles + Haversine math).
--
-- Output layout:
--   {output_dir}/geocoder/country=XX/data_0.parquet
--   {output_dir}/manifest.parquet     (per-country stats)
--   {output_dir}/tile_index.parquet   (per-H3-cell stats)
--
-- Cloud-native query flow (WASM or CLI):
--   1. h3_latlng_to_cell(query_lat, query_lon, 5) → h3_index
--   2. h3_grid_disk(h3_index, 1) → center + 6 neighbors
--   3. Read country file, Parquet pushdown on lat/lon row-group
--      stats prunes to 1-3 row groups (~50K rows each)
--   4. Haversine on lat/lon doubles → nearest addresses
--
-- Enrichments from raw Overture:
--   - lat/lon extracted from geometry (drops spatial dependency)
--   - city/region normalized from address_levels (country-aware)
--   - full_address constructed for text search / JACCARD
--   - h3_index at res 5 for tile-based neighbor queries
--   - geometry/bbox/sources/address_levels dropped (~40% smaller)
-- ============================================================

-- ----------------------------------------------------------
-- Step 1: Enrich Overture addresses into geocoder schema
-- ----------------------------------------------------------
-- Reads source once from S3 (470M rows, ~22 GB) and
-- materializes locally for multi-pass export.

.print '>>> Step 1: Enriching 470M Overture addresses...'

CREATE OR REPLACE TABLE _enriched AS
WITH raw AS (
    SELECT
        id,
        ST_Y(geometry) AS lat,
        ST_X(geometry) AS lon,
        country,
        postcode,
        street,
        number,
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
        id, lat, lon, country, postcode, street, number, unit, h3_index,
        -- City: finest administrative level (country-aware)
        -- 3-level: [1]=region [2]=province [3]=municipality → city = [3]
        -- 2-level: [1]=region [2]=city → city = [2]
        -- 1-level: [1]=city → city = [1]
        -- AT exception: [1]=municipality [2]=sub-locality → city = [1]
        COALESCE(
            CASE
                WHEN country IN ('IT', 'EE', 'LV', 'PL', 'SI', 'SK', 'TW')
                     AND len(address_levels) >= 3
                    THEN address_levels[3].value
                WHEN country = 'AT'
                    THEN address_levels[1].value
                WHEN len(address_levels) >= 2
                    THEN address_levels[2].value
                WHEN len(address_levels) >= 1
                    THEN address_levels[1].value
                ELSE NULL
            END,
            postal_city
        ) AS city,
        -- Region: top-level admin (state/prefecture/region)
        -- 1-level countries have no region
        -- AT: no state-level data in address_levels
        CASE
            WHEN len(address_levels) <= 1 THEN NULL
            WHEN country = 'AT' THEN NULL
            ELSE address_levels[1].value
        END AS region
    FROM raw
)
SELECT
    id, lat, lon, country, postcode, street, number, unit,
    city, region,
    -- Composite address for text search (JACCARD / FTS)
    CONCAT_WS(', ',
        NULLIF(TRIM(COALESCE(number, '') || ' ' || COALESCE(street, '')), ''),
        city,
        region,
        postcode
    ) AS full_address,
    h3_index
FROM with_city_region;

SELECT
    count(*) AS total_enriched,
    count(DISTINCT country) AS countries,
    count(DISTINCT h3_index) AS h3_tiles,
    count(city) AS has_city,
    count(region) AS has_region,
    count(full_address) AS has_full_address
FROM _enriched;

-- ----------------------------------------------------------
-- Step 2: Export geocoder tiles
-- ----------------------------------------------------------
-- Country-partitioned, sorted by (lat, lon) within each
-- partition for maximum row-group spatial pushdown.
--
-- ROW_GROUP_SIZE 50000 → ~1-3 MB per row group → single
-- HTTP range request per group on CDN/S3.
-- PARQUET_VERSION v2 → page-level column indexes.
-- ZSTD level 6 → write-once/read-many, 10-20% smaller than
-- level 3 with no decompression penalty (2-3x slower write
-- is fine for a batch pipeline running once per release).

.print '>>> Step 2: Exporting geocoder tiles (39 country partitions)...'

COPY (
    SELECT
        id, lat, lon, country, postcode, street, number, unit,
        city, region, full_address, h3_index
    FROM _enriched
    ORDER BY lat, lon
) TO (getvariable('output_dir') || '/geocoder/')
(FORMAT PARQUET,
 PARTITION_BY (country),
 PARQUET_VERSION v2,
 COMPRESSION ZSTD,
 COMPRESSION_LEVEL 6,
 ROW_GROUP_SIZE 50000);

-- ----------------------------------------------------------
-- Step 3: Build tile statistics (lightweight aggregation)
-- ----------------------------------------------------------
-- Per-H3-cell stats used by the geocoder SDK to resolve
-- which tiles to fetch for a given query point.

.print '>>> Step 3: Computing tile statistics...'

CREATE OR REPLACE TABLE _tile_stats AS
SELECT
    country,
    h3_index,
    count(*)::INTEGER AS address_count,
    min(lon) AS bbox_min_lon,
    max(lon) AS bbox_max_lon,
    min(lat) AS bbox_min_lat,
    max(lat) AS bbox_max_lat,
    count(DISTINCT postcode) FILTER (postcode IS NOT NULL)::INTEGER AS unique_postcodes
FROM _enriched
GROUP BY country, h3_index;

SELECT count(*) AS total_tiles, sum(address_count) AS total_addresses FROM _tile_stats;

-- ----------------------------------------------------------
-- Step 4: Export manifest (per-country summary)
-- ----------------------------------------------------------
-- Small file (~39 rows) that the SDK fetches first to
-- discover available countries, coverage, and tile counts.

.print '>>> Step 4: Exporting manifest...'

COPY (
    SELECT
        country,
        sum(address_count)::INTEGER AS address_count,
        count(*)::INTEGER AS tile_count,
        min(bbox_min_lon) AS bbox_min_lon,
        max(bbox_max_lon) AS bbox_max_lon,
        min(bbox_min_lat) AS bbox_min_lat,
        max(bbox_max_lat) AS bbox_max_lat,
        5 AS h3_resolution,
        getvariable('overture_release') AS overture_release
    FROM _tile_stats
    GROUP BY country
    ORDER BY address_count DESC
) TO (getvariable('output_dir') || '/manifest.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

-- ----------------------------------------------------------
-- Step 5: Export tile index (per-H3 cell stats)
-- ----------------------------------------------------------
-- SDK uses this to map h3_index → bbox + address count,
-- enabling intelligent tile prefetching and k-ring loading.

.print '>>> Step 5: Exporting tile index...'

COPY (
    SELECT * FROM _tile_stats
    ORDER BY country, h3_index
) TO (getvariable('output_dir') || '/tile_index.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

-- ----------------------------------------------------------
-- Step 6: Cleanup
-- ----------------------------------------------------------

.print '>>> Step 6: Cleanup...'
DROP TABLE _tile_stats;
DROP TABLE _enriched;

.print '>>> GEOCODER BUILD COMPLETE'
