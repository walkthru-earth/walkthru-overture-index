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
--   This gives ~17,500 total files globally:
--   - US: ~3,900 files (~30K addresses each, ~1 MB)
--   - BR: ~4,200 files
--   - DE: ~234 files
--
-- CRS: EPSG:4326 (WGS84) set via ST_SetCRS on geometry
--
-- DuckDB 1.5 features leveraged:
--   - GEOMETRY('EPSG:4326') with CRS metadata
--   - Geometry shredding: Point → STRUCT(x,y) → ALP compressed
--   - Native Parquet 2.11 GeospatialStatistics (bbox/row group)
--   - && operator for spatial filter pushdown
--   - GEOPARQUET_VERSION 'BOTH' for max reader compatibility
--
-- Output layout (written directly to S3 by DuckDB, no Python post-processing):
--   geocoder/country=XX/h3_parent=YYY/data_0.parquet  (Hive tile files)
--   manifest.parquet       (per-country stats — global, 3 KB)
--   tile_index.parquet     (per-h3_parent stats — global, 561 KB)
--   region_index.parquet   (region → tiles — global, 95 KB)
--   city_index/country=XX/data_0.parquet   (per-country cities, Hive)
--   postcode_index/country=XX/data_0.parquet  (per-country postcodes, Hive)
--   street_index/country=XX/data_0.parquet    (per-country streets, Hive)
--   number_index/country=XX/data_0.parquet    (per-country house numbers, Hive)
--
-- Cloud-native query flows:
--
--   Reverse geocode (point → address):
--     1. h3_latlng_to_cell(lat, lon, 5) → h3_index
--     2. h3_cell_to_parent(h3_index, 4) → h3_parent
--     3. Fetch country=XX/h3/YYY.parquet (~1-15 MB)
--     4. WHERE geometry && ST_MakeEnvelope(...)
--     5. ORDER BY ST_Distance_Sphere(geometry, query_point)
--
--   Forward geocode (text → point):
--     1. WASM caches per-country index at country selection:
--        street_index/XX.parquet + postcode_index/XX.parquet
--     2. Parse query → detect type (postcode, street, city)
--     2a. Street:   street_index/XX → 1-2 tiles (instant, in-memory)
--     2b. Postcode: postcode_index/XX → 1-3 tiles (instant, in-memory)
--     2c. City:     city_index (global) → 1-5 tiles (broad fallback)
--     2d. Street+Number: number_index/XX → house number suggestions
--         via HTTP range request (~150 KB, row-group pushdown)
--     3. Fetch only matching tile(s) via HTTP range requests
--     4. ILIKE on full_address within the tile(s)
--
-- Enrichments from raw Overture:
--   - CRS set to EPSG:4326 on geometry
--   - city/region extracted via COALESCE cascade (finest→coarsest level)
--     Fixes OvertureMaps/data#509: LV/SK/EE have variable-depth levels
--   - full_address constructed for text search
--   - h3_index (res 5) + h3_parent (res 4) for spatial tiling
--   - bbox/sources/address_levels dropped (~30% smaller)
--   - h3_index stored as BIGINT (46% smaller, sorted for row-group pushdown)
--
-- Memory optimization:
--   _enriched (469M rows, ~133 GB) is scanned only twice:
--   once for tile export, once for the combined aggregation.
--   Then it is dropped immediately, freeing memory for the
--   index exports which use the much smaller _agg table.
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
        -- JP fix: Overture's number field for Japan is "banchi-coordZone" where
        -- the trailing suffix (1-19) is the MLIT planar rectangular coordinate
        -- system zone (座標系番号), leaked from ISJ source via OpenAddresses.
        -- Example: "362-9" = lot 362 in zone 9 (Kanto). Strip the zone suffix
        -- so the number contains only the real banchi (block/lot number).
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
        -- Keep h3_index as native BIGINT for optimal compression and
        -- row-group min/max pushdown (46% smaller, perfect skip on filter)
        h3_index,
        -- H3 res 4 parent as VARCHAR hex for Hive partition filenames
        h3_h3_to_string(h3_cell_to_parent(h3_index, 4)) AS h3_parent,
        -- City: finest populated administrative level
        -- Overture address_levels is ordered broadest→finest:
        --   [1]=region/state  [2]=district/municipality  [3]=city/village
        -- But depth varies WITHIN a country (see OvertureMaps/data#509):
        --   LV: Rīga has only level1, towns have level2, villages have level3
        --   SK: Bratislava districts are level2, municipalities are level3
        --   EE: towns (linn) are level2, villages (küla) are level3
        -- Universal COALESCE cascade: try finest first, fall back to coarser
        COALESCE(
            CASE WHEN len(address_levels) >= 3 THEN address_levels[3].value END,
            CASE WHEN len(address_levels) >= 2 THEN address_levels[2].value END,
            CASE WHEN len(address_levels) >= 1 THEN address_levels[1].value END,
            postal_city
        ) AS city,
        -- Region: top-level admin (state/prefecture/region)
        -- Only meaningful when there are 2+ levels (otherwise level1 IS the city)
        CASE
            WHEN len(address_levels) >= 2 THEN address_levels[1].value
            ELSE NULL
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
-- Sorted by partition keys + h3_index so:
-- 1. Each partition writes contiguously (no file splitting)
-- 2. Within each tile, rows are grouped by h3_index (res-5 cell)
--    → row groups have tight h3_index min/max ranges
--    → DuckDB-WASM can skip row groups during filter pushdown
--    Without h3_index sort: h3 values spread across ALL row groups,
--    pushdown downloads entire file (~48 MB instead of ~1 MB)

.print '>>> Step 2: Exporting geocoder tiles directly to S3 (country + H3 res 4)...'

-- Write directly to S3 via DuckDB httpfs. No Python post-processing.
-- DuckDB creates Hive-style paths: geocoder/country=XX/h3_parent=YYY/data_0.parquet
-- DuckDB auto-writes bloom filters on dict-encoded columns (street, city, postcode).
-- ROW_GROUP_SIZE 50000 + ORDER BY h3_index = tight min/max for spatial pushdown.

COPY (
    SELECT
        id, geometry, country, postcode, street, number, unit,
        city, region, full_address, h3_index, h3_parent
    FROM _enriched
    ORDER BY country, h3_parent, h3_index
) TO (getvariable('output_dir') || '/geocoder/')
(FORMAT PARQUET,
 PARTITION_BY (country, h3_parent),
 PARQUET_VERSION v2,
 COMPRESSION ZSTD,
 COMPRESSION_LEVEL 6,
 ROW_GROUP_SIZE 50000,
 GEOPARQUET_VERSION 'BOTH',
 OVERWRITE);

-- ----------------------------------------------------------
-- Step 3: Build combined aggregation (single scan of _enriched)
-- ----------------------------------------------------------
-- One scan of 469M rows to compute everything needed for all
-- lookup indexes. This intermediate is ~1-2M rows (vs 469M),
-- so all subsequent index exports are fast.

.print '>>> Step 3: Building combined aggregation (single scan)...'

CREATE OR REPLACE TABLE _agg AS
SELECT
    country,
    h3_parent,
    postcode,
    city,
    region,
    count(*)::INTEGER AS cnt,
    min(ST_X(geometry)) AS min_lon,
    max(ST_X(geometry)) AS max_lon,
    min(ST_Y(geometry)) AS min_lat,
    max(ST_Y(geometry)) AS max_lat
FROM _enriched
GROUP BY country, h3_parent, postcode, city, region;

SELECT count(*) AS agg_rows FROM _agg;

-- Build street aggregation before dropping _enriched.
-- Maps each normalized street name to its tile(s).
-- The WASM client uses this to skip tiles that don't
-- contain the searched street (~2-5 MB file, fetched once).
-- Includes city + centroid for autocomplete disambiguation
-- (primary_city + map pin) — same scan of _enriched, zero extra cost.

.print '>>> Step 3b: Building street aggregation...'

CREATE OR REPLACE TABLE _street_agg AS
SELECT
    country,
    h3_parent,
    lower(street) AS street_lower,
    city,
    count(*)::INTEGER AS cnt,
    avg(ST_X(geometry)) AS centroid_lon,
    avg(ST_Y(geometry)) AS centroid_lat
FROM _enriched
WHERE street IS NOT NULL
GROUP BY country, h3_parent, lower(street), city;

SELECT count(*) AS street_agg_rows FROM _street_agg;

-- Build number aggregation before dropping _enriched.
-- Groups all distinct house numbers per street per country into a sorted list.
-- Used by number_index for address-level autocomplete via HTTP range requests
-- (never bulk-loaded into WASM, queried on-demand with row-group pushdown).

.print '>>> Step 3c: Building number aggregation...'

CREATE OR REPLACE TABLE _number_agg AS
SELECT
    country,
    lower(street) AS street_lower,
    list(DISTINCT number ORDER BY number) AS numbers
FROM _enriched
WHERE street IS NOT NULL AND number IS NOT NULL AND number != ''
GROUP BY country, lower(street);

SELECT count(*) AS number_agg_rows FROM _number_agg;

-- Free the big table immediately (~133 GB)
DROP TABLE _enriched;

.print '>>> _enriched dropped, building indexes from _agg + _street_agg...'

-- ----------------------------------------------------------
-- Step 4: Build tile statistics (from _agg)
-- ----------------------------------------------------------

.print '>>> Step 4: Computing tile statistics...'

CREATE OR REPLACE TABLE _tile_stats AS
WITH tile_agg AS (
    SELECT
        country,
        h3_parent,
        sum(cnt)::INTEGER AS address_count,
        min(min_lon) AS bbox_min_lon,
        max(max_lon) AS bbox_max_lon,
        min(min_lat) AS bbox_min_lat,
        max(max_lat) AS bbox_max_lat,
        count(DISTINCT postcode) FILTER (postcode IS NOT NULL)::INTEGER AS unique_postcodes,
        count(DISTINCT city) FILTER (city IS NOT NULL)::INTEGER AS unique_cities
    FROM _agg
    GROUP BY country, h3_parent
),
tile_regions AS (
    SELECT
        country, h3_parent, region,
        sum(cnt) AS region_count,
        ROW_NUMBER() OVER (PARTITION BY country, h3_parent ORDER BY sum(cnt) DESC) AS rn
    FROM _agg
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
-- Step 5: Export manifest (per-country summary)
-- ----------------------------------------------------------

.print '>>> Step 5: Exporting manifest...'

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
-- Step 6: Export tile index (per-h3_parent stats)
-- ----------------------------------------------------------

.print '>>> Step 6: Exporting tile index...'

COPY (
    SELECT * FROM _tile_stats
    ORDER BY country, h3_parent
) TO (getvariable('output_dir') || '/tile_index.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

DROP TABLE _tile_stats;

-- ----------------------------------------------------------
-- Step 7: Export postcode index — partitioned by country
-- ----------------------------------------------------------
-- Per-country files avoid the 73MB global footer overhead in
-- WASM. The frontend reads postcode_index/NL.parquet directly.

.print '>>> Step 7: Building postcode index (per-country)...'

COPY (
    SELECT
        country,
        postcode,
        list(DISTINCT h3_parent ORDER BY h3_parent) AS tiles,
        sum(cnt)::INTEGER AS addr_count,
        -- Centroid as INT32 scaled (×1e6 ≈ 0.11m precision) for map pin
        ROUND((min(min_lon) + max(max_lon)) / 2 * 1e6)::INTEGER AS centroid_lon_e6,
        ROUND((min(min_lat) + max(max_lat)) / 2 * 1e6)::INTEGER AS centroid_lat_e6
    FROM _agg
    WHERE postcode IS NOT NULL AND postcode != ''
    GROUP BY country, postcode
    ORDER BY country, postcode
) TO (getvariable('output_dir') || '/postcode_index/')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD,
 PARTITION_BY (country), OVERWRITE);

-- ----------------------------------------------------------
-- Step 8: Export region index (from _agg)
-- ----------------------------------------------------------

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
    WHERE region IS NOT NULL
    GROUP BY country, region
    ORDER BY country, region
) TO (getvariable('output_dir') || '/region_index.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

-- ----------------------------------------------------------
-- Step 9: Export city index — partitioned by country
-- ----------------------------------------------------------
-- Per-country files: city_index/NL.parquet (~50 KB) vs
-- the old global city_index.parquet (2.5 MB, 252K rows).
-- WASM reads only the country file — 10x faster (13ms vs 126ms).
-- Includes INT32 bbox for map zoom-to-city in autocomplete.
--
-- Depth-1 countries (FR, ES, NL, etc.) have no region in Overture,
-- so same-name cities in geographically distant areas (e.g. mainland
-- France vs overseas territories) would merge into one row with a
-- planet-spanning bbox. Fix: include LEFT(postcode, 3) in GROUP BY
-- as a geographic disambiguator, but ONLY when region IS NULL.
-- Countries with region data (US, CA, AU, etc.) already disambiguate
-- via the region column. Applying postcode grouping to them fragments
-- cities into many duplicate rows (Ottawa ON had 25 rows, Montreal QC
-- had 95). Using 3 chars (not 2) because FR overseas prefixes 971-988
-- all share "97" but span the globe (971=Guadeloupe, 974=Reunion, etc.).
-- The postcode prefix is NOT stored in the output, only used for grouping.

.print '>>> Step 9: Building city index (per-country)...'

COPY (
    SELECT
        country,
        region,
        city,
        list(DISTINCT h3_parent ORDER BY h3_parent) AS tiles,
        sum(cnt)::INTEGER AS addr_count,
        -- Bbox as INT32 scaled (×1e6) for map zoom-to-city
        ROUND(min(min_lon) * 1e6)::INTEGER AS bbox_min_lon_e6,
        ROUND(max(max_lon) * 1e6)::INTEGER AS bbox_max_lon_e6,
        ROUND(min(min_lat) * 1e6)::INTEGER AS bbox_min_lat_e6,
        ROUND(max(max_lat) * 1e6)::INTEGER AS bbox_max_lat_e6
    FROM _agg
    WHERE city IS NOT NULL
    GROUP BY country, region, city, CASE WHEN region IS NULL THEN LEFT(postcode, 3) ELSE NULL END
    ORDER BY country, city
) TO (getvariable('output_dir') || '/city_index/')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD,
 PARTITION_BY (country), OVERWRITE);

-- ----------------------------------------------------------
-- Step 10: Export street index — partitioned by country
-- ----------------------------------------------------------
-- Per-country files: street_index/NL.parquet (~1 MB) vs
-- the old global street_index.parquet (73.6 MB).
-- WASM reads only the country file — no footer overhead,
-- no row-group pruning needed, single HTTP request.

.print '>>> Step 10: Building street index (per-country)...'

COPY (
    SELECT
        country,
        street_lower,
        list(DISTINCT h3_parent ORDER BY h3_parent) AS tiles,
        sum(cnt)::INTEGER AS addr_count,
        -- primary_city: city where this street has the most addresses
        arg_max(city, cnt) AS primary_city,
        -- Weighted centroid as INT32 scaled (×1e6) for map pin
        ROUND(sum(centroid_lon * cnt) / sum(cnt) * 1e6)::INTEGER AS centroid_lon_e6,
        ROUND(sum(centroid_lat * cnt) / sum(cnt) * 1e6)::INTEGER AS centroid_lat_e6
    FROM _street_agg
    GROUP BY country, street_lower
    ORDER BY country, street_lower
) TO (getvariable('output_dir') || '/street_index/')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD,
 PARTITION_BY (country), OVERWRITE);

DROP TABLE _street_agg;

-- ----------------------------------------------------------
-- Step 11: Export number index — partitioned by country
-- ----------------------------------------------------------
-- Per-country files: number_index/country=XX/data_0.parquet (~9 MB on disk).
-- NOT bulk-loaded into WASM at prefetch (unlike street/postcode indexes).
-- Queried on-demand via read_parquet() with HTTP range requests when
-- the user types a street + number (autocomplete "ready" mode).
--
-- Written directly to S3 by DuckDB, preserving:
--   - ROW_GROUP_SIZE 2000 (narrow street_lower ranges per group)
--   - Bloom filters on dict-encoded columns (automatic in DuckDB 1.2+)
--   - Sorted by street_lower for tight min/max row-group stats
--
-- DuckDB-WASM flow for "keizersgracht 18":
--   1. Fetch Parquet footer (~1 KB, cached after first query)
--   2. Bloom filter check on street_lower → skip ~67/68 groups
--   3. Row-group min/max pushdown → confirm 1-2 matching groups
--   4. Fetch only matching row group(s) (~100-150 KB range request)
--   5. Filter numbers[] array client-side for prefix "18"
--   6. Show: keizersgracht 180, 181, 185...
--
-- Compared to fetching a full tile (0.5-15 MB), this is 50-100x smaller.

.print '>>> Step 11: Building number index (per-country)...'

COPY (
    SELECT country, street_lower, numbers
    FROM _number_agg
    ORDER BY country, street_lower
) TO (getvariable('output_dir') || '/number_index/')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD,
 ROW_GROUP_SIZE 2000,
 PARTITION_BY (country), OVERWRITE);

DROP TABLE _number_agg;

-- ----------------------------------------------------------
-- Step 12: Cleanup
-- ----------------------------------------------------------

.print '>>> Step 12: Cleanup...'
DROP TABLE _agg;

.print '>>> GEOCODER BUILD COMPLETE'
