-- ============================================================
-- Walkthru Cloud-Native Address Geocoder — DuckDB 1.5
--
-- Source:  Overture Maps addresses (direct from S3)
-- Output: Two-level partitioned GeoParquet geocoder tiles
--         optimized for both reverse AND forward geocoding
--         in DuckDB-WASM via HTTP range requests
--
-- Partitioning strategy (inspired by @tabaqat/geocoding-sdk):
--   Level 1: country (Hive partition) → eliminates 38/39 dirs
--   Level 2: h3_parent (H3 res 4, ~1,770 km²) → ~0.5-15 MB
--            per file, small enough for WASM FTS
--
--   This gives ~4,000-6,000 total files globally:
--   - US: ~2,500 files (~50K addresses each, ~1 MB)
--   - NL: ~20 files (~500K each, ~11 MB)
--   - LI: 1 file (13K addresses, ~300 KB)
--
-- CRS: EPSG:4326 (WGS84) set via ST_SetCRS on geometry
--
-- DuckDB 1.5 features leveraged:
--   - GEOMETRY('EPSG:4326') with CRS metadata
--   - Geometry shredding: Point → STRUCT(x,y) → ALP compressed
--   - Native Parquet 2.11 GeospatialStatistics (bbox/row group)
--   - && operator for spatial filter pushdown
--   - ST_Hilbert() for spatial sort order
--   - GEOPARQUET_VERSION 'BOTH' for max reader compatibility
--
-- Output layout:
--   geocoder/country=XX/h3_parent=YYY/data_0.parquet
--   manifest.parquet       (per-country stats)
--   tile_index.parquet     (per-h3_parent tile stats)
--   postcode_index.parquet (postcode → tile mapping)
--   region_index.parquet   (region → tile mapping)
--   city_index.parquet     (city → tile mapping)
--
-- Cloud-native query flows:
--
--   Reverse geocode (point → address):
--     1. h3_latlng_to_cell(lat, lon, 5) → h3_index
--     2. h3_cell_to_parent(h3_index, 4) → h3_parent
--     3. Fetch country=XX/h3_parent=YYY/data_0.parquet (~1-15 MB)
--     4. WHERE geometry && ST_MakeEnvelope(...)
--     5. ORDER BY ST_Distance_Sphere(geometry, query_point)
--
--   Forward geocode (text → point):
--     1. Parse query → detect type (postcode, region, city, address)
--     2a. Postcode: postcode_index → 1-3 tiles (~fastest)
--     2b. Region:   region_index → 5-50 tiles (+ bbox filter)
--     2c. City:     city_index → 1-5 tiles
--     2d. Address:  tile_index → filter by bbox/region
--     3. Fetch those tile files (~1-15 MB each)
--     4. FTS/BM25 on full_address or JACCARD fallback
--
-- Enrichments from raw Overture:
--   - CRS set to EPSG:4326 on geometry
--   - city/region normalized from address_levels (country-aware)
--   - full_address constructed for text search
--   - h3_index (res 5) + h3_parent (res 4) for spatial tiling
--   - bbox/sources/address_levels dropped (~30% smaller)
--   - h3_index stored as hex string (no h3 extension needed)
-- ============================================================

-- ----------------------------------------------------------
-- Step 1: Enrich Overture addresses into geocoder schema
-- ----------------------------------------------------------
-- Reads source once from S3 (470M rows, ~22 GB).
-- Sets CRS to EPSG:4326 on geometry column.
-- Computes both h3_index (res 5) for point queries and
-- h3_parent (res 4) for file-level partitioning.

.print '>>> Step 1: Enriching 470M Overture addresses...'

CREATE OR REPLACE TABLE _enriched AS
WITH raw AS (
    SELECT
        id,
        ST_SetCRS(geometry, 'EPSG:4326') AS geometry,
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
        id, geometry, country, postcode, street, number, unit,
        -- Convert H3 BIGINT to hex string so WASM consumers
        -- don't need the h3 extension to read the value
        h3_h3_to_string(h3_index) AS h3_index,
        -- H3 res 4 parent for file-level partitioning (~1,770 km² per cell)
        h3_h3_to_string(h3_cell_to_parent(h3_index, 4)) AS h3_parent,
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
        CASE
            WHEN len(address_levels) <= 1 THEN NULL
            WHEN country = 'AT' THEN NULL
            ELSE address_levels[1].value
        END AS region
    FROM raw
)
SELECT
    id, geometry, country, postcode, street, number, unit,
    city, region,
    -- Country-aware address formatting:
    -- Street-first: DE, FR, IT, NL, AT, CH, ES, PT, BE, LU, NO, SE,
    --               FI, DK, IS, PL, CZ, SK, SI, HR, RS, EE, LV, LT
    -- Number-first: US, CA, AU, NZ, GB, IE, BR, MX, CL, CO, UY, SG
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
    count(DISTINCT h3_parent) AS h3_parent_tiles,
    count(DISTINCT h3_index) AS h3_res5_cells,
    count(city) AS has_city,
    count(full_address) AS has_full_address,
    ST_CRS(first(geometry)) AS crs
FROM _enriched;

-- ----------------------------------------------------------
-- Step 2: Export geocoder tiles
-- ----------------------------------------------------------
-- Two-level partitioning: country + h3_parent (H3 res 4)
-- Each file is ~0.5-15 MB, small enough for WASM FTS.
-- No global sort needed: each tile is already a small H3 res 4
-- cell (~1,770 km²) so spatial locality is inherent.
-- GeoParquet bbox stats are written per row group regardless.

.print '>>> Step 2: Exporting geocoder tiles to local scratch (country + H3 res 4)...'

-- Write to local scratch dir first. Python post-processing
-- flattens h3_parent=XXX/data_0.parquet → h3_parent=XXX.parquet
-- then uploads to S3. This avoids the Hive directory nesting
-- that PARTITION_BY creates.

COPY (
    SELECT
        id, geometry, country, postcode, street, number, unit,
        city, region, full_address, h3_index, h3_parent
    FROM _enriched
) TO (getvariable('scratch_dir') || '/geocoder/')
(FORMAT PARQUET,
 PARTITION_BY (country, h3_parent),
 PARQUET_VERSION v2,
 COMPRESSION ZSTD,
 COMPRESSION_LEVEL 6,
 ROW_GROUP_SIZE 50000,
 GEOPARQUET_VERSION 'BOTH',
 OVERWRITE);

-- ----------------------------------------------------------
-- Step 3: Build tile statistics
-- ----------------------------------------------------------
-- Per-h3_parent stats for tile discovery. The SDK uses this
-- to map a query point → h3_parent → file path.

.print '>>> Step 3: Computing tile statistics...'

CREATE OR REPLACE TABLE _tile_stats AS
WITH tile_agg AS (
    SELECT
        country,
        h3_parent,
        count(*)::INTEGER AS address_count,
        min(ST_X(geometry)) AS bbox_min_lon,
        max(ST_X(geometry)) AS bbox_max_lon,
        min(ST_Y(geometry)) AS bbox_min_lat,
        max(ST_Y(geometry)) AS bbox_max_lat,
        count(DISTINCT postcode) FILTER (postcode IS NOT NULL)::INTEGER AS unique_postcodes,
        count(DISTINCT city) FILTER (city IS NOT NULL)::INTEGER AS unique_cities
    FROM _enriched
    GROUP BY country, h3_parent
),
-- Dominant region per tile (most addresses wins)
tile_regions AS (
    SELECT
        country, h3_parent, region,
        count(*) AS region_count,
        ROW_NUMBER() OVER (PARTITION BY country, h3_parent ORDER BY count(*) DESC) AS rn
    FROM _enriched
    WHERE region IS NOT NULL
    GROUP BY country, h3_parent, region
)
SELECT
    t.*,
    r.region AS primary_region
FROM tile_agg t
LEFT JOIN tile_regions r
    ON t.country = r.country AND t.h3_parent = r.h3_parent AND r.rn = 1;

SELECT
    count(*) AS total_tiles,
    sum(address_count) AS total_addresses,
    avg(address_count)::INTEGER AS avg_per_tile,
    max(address_count) AS max_per_tile,
    min(address_count) AS min_per_tile
FROM _tile_stats;

-- ----------------------------------------------------------
-- Step 4: Export manifest (per-country summary)
-- ----------------------------------------------------------

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
        4 AS h3_parent_resolution,
        5 AS h3_index_resolution,
        getvariable('overture_release') AS overture_release
    FROM _tile_stats
    GROUP BY country
    ORDER BY address_count DESC
) TO (getvariable('output_dir') || '/manifest.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

-- ----------------------------------------------------------
-- Step 5: Export tile index (per-h3_parent stats)
-- ----------------------------------------------------------
-- SDK fetches this once (~200 KB), caches it, then uses
-- it to resolve any h3_index → h3_parent → file URL.

.print '>>> Step 5: Exporting tile index...'

COPY (
    SELECT * FROM _tile_stats
    ORDER BY country, h3_parent
) TO (getvariable('output_dir') || '/tile_index.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

-- ----------------------------------------------------------
-- Step 6: Export postcode index (for forward geocoding)
-- ----------------------------------------------------------
-- Maps each postcode to the h3_parent tile(s) containing it.
-- Most postcodes span only 1-3 tiles. This allows the SDK
-- to jump straight to the right tile(s) for postcode queries
-- without scanning all tiles in a country.

.print '>>> Step 6: Building postcode index...'

COPY (
    SELECT
        country,
        postcode,
        list(DISTINCT h3_parent ORDER BY h3_parent) AS tiles,
        count(*)::INTEGER AS addr_count
    FROM _enriched
    WHERE postcode IS NOT NULL AND postcode != ''
    GROUP BY country, postcode
    ORDER BY country, postcode
) TO (getvariable('output_dir') || '/postcode_index.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

-- ----------------------------------------------------------
-- Step 7: Export region index (for forward geocoding)
-- ----------------------------------------------------------
-- Maps each region to the h3_parent tile(s) containing it.
-- Enables "search California" → jump to the right tiles.
-- Most regions span 5-50 tiles. ~500 regions globally.

.print '>>> Step 7: Building region index...'

COPY (
    SELECT
        country,
        region,
        list(DISTINCT h3_parent ORDER BY h3_parent) AS tiles,
        count(*)::INTEGER AS addr_count,
        min(ST_X(geometry)) AS bbox_min_lon,
        max(ST_X(geometry)) AS bbox_max_lon,
        min(ST_Y(geometry)) AS bbox_min_lat,
        max(ST_Y(geometry)) AS bbox_max_lat
    FROM _enriched
    WHERE region IS NOT NULL
    GROUP BY country, region
    ORDER BY country, region
) TO (getvariable('output_dir') || '/region_index.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

-- ----------------------------------------------------------
-- Step 8: Export city index (for forward geocoding)
-- ----------------------------------------------------------
-- Maps each city to the h3_parent tile(s) containing it.
-- Enables "search Amsterdam" → jump to 1-5 tiles.
-- Most cities fit in 1-3 tiles. ~50K cities globally.

.print '>>> Step 8: Building city index...'

COPY (
    SELECT
        country,
        region,
        city,
        list(DISTINCT h3_parent ORDER BY h3_parent) AS tiles,
        count(*)::INTEGER AS addr_count
    FROM _enriched
    WHERE city IS NOT NULL
    GROUP BY country, region, city
    ORDER BY country, city
) TO (getvariable('output_dir') || '/city_index.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

-- ----------------------------------------------------------
-- Step 9: Cleanup
-- ----------------------------------------------------------

.print '>>> Step 9: Cleanup...'
DROP TABLE _tile_stats;
DROP TABLE _enriched;

.print '>>> GEOCODER BUILD COMPLETE'
