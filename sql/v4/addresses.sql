-- ============================================================
-- Walkthru Cloud-Native Address Geocoder v4 — DuckDB 1.5
--
-- Source:  Overture Maps addresses (direct from S3)
-- Output: Three-level partitioned GeoParquet geocoder tiles
--         optimized for both forward AND reverse geocoding
--         in DuckDB-WASM via HTTP range requests
--
-- ============================================================
-- WHY v4? (Investigation results, 2026-04-04)
--
-- v1 tiles are sorted by h3_index (BIGINT, H3 res-5). This
-- gives excellent reverse-geocoding pushdown (0.9s, 64 KiB,
-- 1 GET on a 1.1M-row NL mega-tile) but BREAKS forward
-- geocoding: street values are randomly distributed across
-- h3-sorted row groups, so DuckDB must scan EVERY row group
-- for a street filter. Result: 6.4s, 3.5 MiB, 45 GETs on
-- the same NL tile, just to find "keizersgracht".
--
-- 74.9% of all addresses (351M of 469M) live in tiles with
-- >50K rows. These are the dense urban areas where autocomplete
-- matters most. The v1 architecture makes autocomplete unusable
-- in exactly the places it's needed most.
--
-- v4 fixes this with two changes:
--   1. Sort tiles by lower(street) ASC, number ASC
--      -> street min/max pushdown skips ~95% of row groups
--      -> 1-2 row groups per forward query (~100 KB, 1-2 GETs)
--   2. Sub-partition mega-tiles into ~50K-row buckets
--      -> reverse geocoding full-scans a 50K bucket (~500 KB)
--      -> no h3_index pushdown needed at that size
-- ============================================================
--
-- Partitioning strategy (three levels):
--   Level 1: country (Hive partition) -> eliminates 38/39 dirs
--   Level 2: h3_res4 (H3 res-4 hex, ~1,770 km²)
--   Level 3: bucket (NTILE over h3_index, only for mega-tiles)
--
-- Normal tiles (<=50K rows):
--   geocoder/country=XX/h3_res4=YYY/bucket=_/data_0.parquet
--   Single file, street-sorted, 1-5 row groups.
--   Forward: 1 row group per street search (~100 KB)
--   Reverse: full scan is fine (50K rows = ~500 KB)
--
-- Mega-tiles (>50K rows, ~2200 tiles covering 351M addresses):
--   geocoder/country=XX/h3_res4=YYY/bucket=01/data_0.parquet
--   geocoder/country=XX/h3_res4=YYY/bucket=02/data_0.parquet
--   ...
--   Each bucket has ~50K rows covering a contiguous spatial
--   region (NTILE over h3_index preserves spatial locality).
--   Forward: street_index -> h3_res4 -> fetch the right bucket
--            -> street pushdown -> 1 row group (~100 KB)
--   Reverse: tile_index has h3_index_min/max per bucket
--            -> identify which bucket covers the query point
--            -> full scan ~50K rows (~500 KB)
--
-- WHY NTILE over h3_index instead of H3 res-5/res-6:
--   H3 res-5 gives only 7 child cells per res-4, with HIGHLY
--   skewed distribution. Validated on live data:
--     BR Sao Paulo: 7 res-5 cells, max 806K in ONE cell
--     NL Rotterdam: 7 res-5 cells, max 349K in one cell
--   NTILE guarantees exactly even bucket sizes (~50K each).
--   h3_index ordering makes each bucket a contiguous spatial
--   region, so the tile_index can map any h3 cell to a bucket
--   via h3_index_min/h3_index_max range lookup.
--   BR Sao Paulo: 66 buckets x 50K = 3.3M (vs 7 skewed cells)
--
-- WHY sort by lower(street) ASC, not h3_index:
--   Validated via EXPLAIN ANALYZE on live v1 data:
--
--   h3-sorted NL mega-tile (1.1M rows):
--     Forward (street filter): 6.42s, 3.5 MiB, 45 GETs
--     Reverse (h3 filter):     0.90s, 64 KiB, 1 GET
--
--   With street sort (simulated row-group ranges):
--     44 row groups, each spans ~400 unique streets
--     "keizersgracht" fits in exactly 1 row group
--     Expected forward: ~100 KB, 1-2 GETs, <0.5s
--
--   Reverse geocoding loses h3 pushdown but gains from small
--   bucket size. Full scan of 50K rows = ~500 KB, 3-5 GETs.
--   Acceptable tradeoff: reverse is a single click action,
--   forward is typed on every keystroke.
--
-- WHY ASC not DESC for street sort:
--   1. min/max pushdown works identically either way
--   2. LIMIT optimization: autocomplete queries use
--      ORDER BY street, number LIMIT 15. ASC sort matches
--      this order, enabling streaming limit (stop after
--      first matching row group emits 15 rows). DESC would
--      require reading ALL matching groups then reversing.
--   3. Prefix clustering: "rua a..." through "rua z..." are
--      contiguous in ASC, so LIKE 'rua d%' hits fewer groups.
--
-- WHY ROW_GROUP_SIZE 10000:
--   Smaller row groups = tighter street min/max ranges per
--   group = fewer false-positive groups fetched. At ~80
--   bytes/row compressed, 10K rows = ~80 KB per range request.
--   v1 used 25K (after tuning from 50K). Going to 10K gives
--   ~200 unique streets per group (vs ~400 at 25K), cutting
--   false positives in half. The footer overhead per extra
--   row group is ~200 bytes, negligible vs the ~500 KB file.
--
-- WHY drop id, country, h3_parent from tiles:
--   - id (UUID): never used in any query, ~15% of file size
--   - country: always the same value (it's in the file path)
--   - h3_parent: always the same value (it's in the file path)
--   Together these save ~25-30% of compressed tile size.
--   Validated via SUMMARIZE: country and h3_parent have
--   approx_unique=1 in every tile.
--
-- GEOPARQUET_VERSION 'BOTH':
--   DuckDB 1.5 writes geometry as STRUCT(x,y) with ALP
--   compression (~40% smaller than WKB) and per-row-group
--   GeospatialStatistics (bbox in Parquet footer). This
--   enables spatial pushdown via the && operator.
--   'BOTH' also writes GeoParquet v1 metadata for readers
--   that don't support native Parquet 2.11 geo stats.
--   IMPORTANT: the merge step in main.py must preserve
--   GEOPARQUET_VERSION 'BOTH' when re-writing split
--   partition files. Omitting it produces WKB blobs with
--   no bbox stats (confirmed: v1 files on S3 show
--   geo_bbox=NULL because earlier merge steps lacked this).
--
-- Output layout:
--   geocoder/country=XX/h3_res4=YYY/bucket=ZZ/data_0.parquet
--   manifest.parquet       (per-country stats, 39 rows, ~3 KB)
--   tile_index.parquet     (per-bucket stats with h3_index ranges)
--   region_index.parquet   (region -> tiles, global, ~95 KB)
--   city_index/country=XX/data_0.parquet   (per-country)
--   postcode_index/country=XX/data_0.parquet  (per-country)
--   street_index/country=XX/data_0.parquet    (per-country)
--   number_index/country=XX/data_0.parquet    (per-country)
--
-- Cloud-native query flows:
--
--   Forward geocode (autocomplete, typed every keystroke):
--     1. WASM caches per-country indexes at country selection:
--        street_index/XX, postcode_index/XX, city_index/XX
--     2. User types -> classifyInput() -> mode
--     3. street_index lookup -> h3_res4 tile(s) containing street
--     4. tile_index lookup -> which bucket(s) in that h3_res4
--        (for normal tiles: bucket='_', for mega: all buckets
--         contain the street since each bucket is spatial, not
--         text-partitioned. The street_index already narrowed
--         to the right h3_res4. Within each bucket, street
--         pushdown skips to the right row group.)
--     5. Fetch bucket file(s) -> WHERE street_lower = X
--        -> min/max pushdown -> 1 row group (~100 KB)
--     6. For street+number: number_index via HTTP range request
--
--   Reverse geocode (single map click):
--     1. h3_latlng_to_cell(lat, lon, 5) -> h3_index
--     2. h3_cell_to_parent(h3_index, 4) -> h3_res4
--     3. tile_index lookup -> find bucket where
--        h3_index_min <= h3_index <= h3_index_max
--     4. Fetch that bucket -> full scan (~50K rows, ~500 KB)
--     5. Compute distance, sort, return nearest
--
-- Memory optimization:
--   _enriched (469M rows, ~133 GB) is scanned 3 times:
--   1. Count per h3_res4 (to identify mega-tiles)
--   2. Assign NTILE buckets to mega-tile rows
--   3. Combined aggregation for all indexes
--   Then it is dropped immediately.
-- ============================================================

-- ----------------------------------------------------------
-- Step 1: Enrich Overture addresses into geocoder schema
-- ----------------------------------------------------------
-- Reads source once from S3 (470M rows, ~22 GB).
-- Sets CRS to EPSG:4326 on geometry column.
-- Computes h3_index (res 5) for spatial bucketing and
-- h3_res4 (res 4 hex) for file-level partitioning.

.print '>>> Step 1: Enriching 470M Overture addresses...'

CREATE OR REPLACE TABLE _enriched AS
WITH raw AS (
    SELECT
        ST_SetCRS(geometry, 'EPSG:4326') AS geometry,
        country,
        postcode,
        street,
        -- JP fix: Overture's number field for Japan is "banchi-coordZone" where
        -- the trailing suffix (1-19) is the MLIT planar rectangular coordinate
        -- system zone, leaked from ISJ source via OpenAddresses.
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
        geometry, country, postcode, street, number, unit,
        -- Keep h3_index as native BIGINT for:
        -- 1. NTILE bucketing (contiguous spatial ranges)
        -- 2. tile_index h3_index_min/max for reverse geocode lookup
        -- 3. 46% smaller than VARCHAR hex
        h3_index,
        -- H3 res 4 parent as VARCHAR hex for Hive partition filenames
        h3_h3_to_string(h3_cell_to_parent(h3_index, 4)) AS h3_res4,
        -- City: finest populated administrative level
        -- Overture address_levels is ordered broadest->finest:
        --   [1]=region/state  [2]=district/municipality  [3]=city/village
        -- But depth varies WITHIN a country (see OvertureMaps/data#509):
        --   LV: Riga has only level1, towns have level2, villages have level3
        --   SK: Bratislava districts are level2, municipalities are level3
        --   EE: towns (linn) are level2, villages (kula) are level3
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
    geometry, country, postcode, street, number, unit,
    city, region,
    -- Country-aware address formatting:
    -- Number-first: US, CA, AU, NZ, BR, MX, CL, CO, UY, SG, HK, TW
    -- Street-first: everyone else (DE, FR, IT, NL, etc.)
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
    h3_res4
FROM with_city_region;

SELECT
    count(*) AS total_enriched,
    count(DISTINCT country) AS countries,
    count(DISTINCT h3_res4) AS h3_res4_tiles,
    count(DISTINCT h3_index) AS h3_res5_cells,
    count(city) AS has_city,
    count(full_address) AS has_full_address,
    ST_CRS(first(geometry)) AS crs
FROM _enriched;

-- ----------------------------------------------------------
-- Step 2: Identify mega-tiles and assign spatial buckets
-- ----------------------------------------------------------
-- Count addresses per h3_res4 tile. Tiles above 50K rows get
-- sub-partitioned into ~50K-row buckets using NTILE over
-- h3_index. This guarantees even bucket sizes while preserving
-- spatial locality (because h3_index is a space-filling curve).
--
-- Why 50K threshold:
--   - 50K rows x ~80 bytes/row = ~400 KB compressed per bucket
--   - Full scan for reverse geocoding: 3-5 HTTP range requests
--   - Forward geocoding: 1 row group (~100 KB) via street pushdown
--   - Below 50K, sub-partitioning adds file overhead for no gain

.print '>>> Step 2: Identifying mega-tiles and assigning buckets...'

CREATE OR REPLACE TABLE _tile_sizes AS
SELECT h3_res4, count(*) AS addr_count
FROM _enriched
GROUP BY h3_res4;

-- Show the distribution before bucketing
SELECT
    count(*) AS total_tiles,
    count(*) FILTER (addr_count <= 50000) AS normal_tiles,
    count(*) FILTER (addr_count > 50000) AS mega_tiles,
    sum(addr_count) FILTER (addr_count > 50000) AS addr_in_mega
FROM _tile_sizes;

-- Assign bucket numbers to mega-tile rows.
-- Normal tiles get bucket='_' (sentinel for single-file partition).
-- Mega-tiles get bucket='01','02',...,'NN' using NTILE.
-- NTILE over h3_index ensures each bucket is a contiguous spatial
-- region with approximately equal row count.
--
-- We use LPAD to zero-pad bucket numbers so they sort correctly
-- as strings in file paths (bucket=01 before bucket=10).

CREATE OR REPLACE TABLE _bucketed AS
SELECT
    e.*,
    CASE
        WHEN t.addr_count <= 50000 THEN '_'
        ELSE LPAD(
            NTILE(CEIL(t.addr_count / 50000.0)::INTEGER)
                OVER (PARTITION BY e.h3_res4 ORDER BY e.h3_index)
            ::VARCHAR,
            CEIL(LOG10(CEIL(t.addr_count / 50000.0) + 1))::INTEGER,
            '0'
        )
    END AS bucket
FROM _enriched e
JOIN _tile_sizes t ON e.h3_res4 = t.h3_res4;

DROP TABLE _enriched;
DROP TABLE _tile_sizes;

.print '>>> Buckets assigned. Verifying distribution...'

SELECT
    count(DISTINCT h3_res4 || '/' || bucket) AS total_files,
    count(*) FILTER (bucket = '_') AS addr_in_normal,
    count(*) FILTER (bucket != '_') AS addr_in_bucketed,
    min(cnt) AS min_bucket_size,
    max(cnt) AS max_bucket_size,
    avg(cnt)::INTEGER AS avg_bucket_size
FROM (
    SELECT h3_res4, bucket, count(*) AS cnt
    FROM _bucketed
    GROUP BY h3_res4, bucket
);

-- ----------------------------------------------------------
-- Step 3: Export geocoder tiles
-- ----------------------------------------------------------
-- Three-level partitioning: country + h3_res4 + bucket
-- Sorted by lower(street) ASC, number ASC within each file.
--
-- Street sort enables min/max row-group pushdown for forward
-- geocoding: a WHERE lower(street) = 'keizersgracht' filter
-- skips ~95% of row groups because each group spans only
-- ~200 unique streets (at ROW_GROUP_SIZE 10000).
--
-- Columns dropped from tiles (validated via SUMMARIZE):
--   - id: UUID, never queried, ~15% of file size
--   - country: approx_unique=1 per tile (redundant with path)
--   - h3_res4: approx_unique=1 per tile (redundant with path)
--
-- h3_index is kept in tiles for:
--   - Reverse geocoding: client can filter by h3_index after
--     full scan for extra precision within a bucket
--   - Future use: if DuckDB-WASM adds secondary index support
--
-- street_lower (added in v4.1): pre-computed lower(street) as a
-- physical column. DuckDB-WASM queries filter on street_lower
-- instead of lower(street) so Parquet row-group min/max stats
-- can push down the filter. Without this, lower() applied at
-- query time defeats pushdown because DuckDB can't apply
-- functions to stored statistics. With street_lower, a query
-- like WHERE street_lower LIKE 'via cave%' checks the row
-- group min/max directly, skipping ~95% of row groups.

.print '>>> Step 3: Exporting geocoder tiles (street-sorted, bucketed)...'

COPY (
    SELECT
        country, h3_res4, bucket,
        geometry, postcode, street, number, unit,
        city, region, full_address, h3_index,
        lower(street) AS street_lower
    FROM _bucketed
    ORDER BY country, h3_res4, bucket, lower(street), number
) TO (getvariable('output_dir') || '/geocoder/')
(FORMAT PARQUET,
 PARTITION_BY (country, h3_res4, bucket),
 PARQUET_VERSION v2,
 COMPRESSION ZSTD,
 COMPRESSION_LEVEL 6,
 ROW_GROUP_SIZE 10000,
 GEOPARQUET_VERSION 'BOTH',
 OVERWRITE);

-- ----------------------------------------------------------
-- Step 4: Build combined aggregation (single scan)
-- ----------------------------------------------------------
-- One scan of _bucketed to compute everything needed for all
-- lookup indexes. This intermediate is ~1-2M rows (vs 469M),
-- so all subsequent index exports are fast.

.print '>>> Step 4: Building combined aggregation (single scan)...'

CREATE OR REPLACE TABLE _agg AS
SELECT
    country,
    h3_res4,
    postcode,
    city,
    region,
    count(*)::INTEGER AS cnt,
    min(ST_X(geometry)) AS min_lon,
    max(ST_X(geometry)) AS max_lon,
    min(ST_Y(geometry)) AS min_lat,
    max(ST_Y(geometry)) AS max_lat
FROM _bucketed
GROUP BY country, h3_res4, postcode, city, region;

SELECT count(*) AS agg_rows FROM _agg;

-- Street aggregation (for street_index export)
.print '>>> Step 4b: Building street aggregation...'

CREATE OR REPLACE TABLE _street_agg AS
SELECT
    country,
    h3_res4,
    lower(street) AS street_lower,
    city,
    count(*)::INTEGER AS cnt,
    avg(ST_X(geometry)) AS centroid_lon,
    avg(ST_Y(geometry)) AS centroid_lat
FROM _bucketed
WHERE street IS NOT NULL
GROUP BY country, h3_res4, lower(street), city;

SELECT count(*) AS street_agg_rows FROM _street_agg;

-- Number aggregation (for number_index export)
.print '>>> Step 4c: Building number aggregation...'

CREATE OR REPLACE TABLE _number_agg AS
SELECT
    country,
    lower(street) AS street_lower,
    list(DISTINCT number ORDER BY number) AS numbers
FROM _bucketed
WHERE street IS NOT NULL AND number IS NOT NULL AND number != ''
GROUP BY country, lower(street);

SELECT count(*) AS number_agg_rows FROM _number_agg;

-- Bucket-level stats (for tile_index with h3_index ranges)
-- This is the key table for reverse geocode bucket lookup:
-- given an h3_index, find which (h3_res4, bucket) contains it.
.print '>>> Step 4d: Building bucket stats...'

CREATE OR REPLACE TABLE _bucket_stats AS
SELECT
    country,
    h3_res4,
    bucket,
    count(*)::INTEGER AS address_count,
    min(h3_index) AS h3_index_min,
    max(h3_index) AS h3_index_max,
    min(ST_X(geometry)) AS bbox_min_lon,
    max(ST_X(geometry)) AS bbox_max_lon,
    min(ST_Y(geometry)) AS bbox_min_lat,
    max(ST_Y(geometry)) AS bbox_max_lat,
    count(DISTINCT postcode) FILTER (postcode IS NOT NULL)::INTEGER AS unique_postcodes,
    count(DISTINCT city) FILTER (city IS NOT NULL)::INTEGER AS unique_cities
FROM _bucketed
GROUP BY country, h3_res4, bucket;

SELECT
    count(*) AS total_entries,
    count(*) FILTER (bucket = '_') AS normal_entries,
    count(*) FILTER (bucket != '_') AS bucket_entries,
    sum(address_count) AS total_addresses,
    avg(address_count)::INTEGER AS avg_per_entry,
    max(address_count) AS max_per_entry
FROM _bucket_stats;

-- Free the big table (~133 GB)
DROP TABLE _bucketed;
.print '>>> _bucketed dropped, building indexes from aggregation tables...'

-- ----------------------------------------------------------
-- Step 5: Export tile_index (with bucket-level h3_index ranges)
-- ----------------------------------------------------------
-- The SDK fetches this once (~500 KB), caches it, then uses
-- it for both forward and reverse geocode tile resolution.
--
-- For reverse geocoding, the client computes h3_index for the
-- query point, then searches tile_index for the row where
-- h3_index_min <= query_h3 <= h3_index_max. This gives the
-- (country, h3_res4, bucket) triple needed to construct the
-- file URL.
--
-- For forward geocoding, the client uses h3_res4 from
-- street_index to look up all buckets for that tile. It then
-- fetches each bucket with street pushdown.

.print '>>> Step 5: Exporting tile index...'

-- Add primary_region via correlated lookup from _agg
CREATE OR REPLACE TABLE _tile_index AS
SELECT
    b.*,
    (SELECT a.region FROM _agg a
     WHERE a.country = b.country AND a.h3_res4 = b.h3_res4
       AND a.region IS NOT NULL
     GROUP BY a.region ORDER BY sum(a.cnt) DESC LIMIT 1
    ) AS primary_region
FROM _bucket_stats b;

COPY (
    SELECT * FROM _tile_index
    ORDER BY country, h3_res4, bucket
) TO (getvariable('output_dir') || '/tile_index.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

DROP TABLE _bucket_stats;

-- ----------------------------------------------------------
-- Step 6: Export manifest (per-country summary)
-- ----------------------------------------------------------

.print '>>> Step 6: Exporting manifest...'

COPY (
    SELECT
        country,
        sum(address_count)::INTEGER AS address_count,
        count(*)::INTEGER AS tile_count,
        count(*) FILTER (bucket != '_')::INTEGER AS bucket_count,
        count(DISTINCT h3_res4)::INTEGER AS h3_res4_count,
        min(bbox_min_lon) AS bbox_min_lon,
        max(bbox_max_lon) AS bbox_max_lon,
        min(bbox_min_lat) AS bbox_min_lat,
        max(bbox_max_lat) AS bbox_max_lat,
        4 AS h3_parent_resolution,
        5 AS h3_index_resolution,
        getvariable('overture_release') AS overture_release
    FROM _tile_index
    GROUP BY country
    ORDER BY address_count DESC
) TO (getvariable('output_dir') || '/manifest.parquet')
(FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD);

DROP TABLE _tile_index;

-- ----------------------------------------------------------
-- Step 7: Export postcode index (per-country, Hive)
-- ----------------------------------------------------------
-- Per-country files avoid the 73MB global footer overhead in
-- WASM. The frontend reads postcode_index/NL.parquet directly.

.print '>>> Step 7: Building postcode index (per-country)...'

COPY (
    SELECT
        country,
        postcode,
        list(DISTINCT h3_res4 ORDER BY h3_res4) AS tiles,
        sum(cnt)::INTEGER AS addr_count,
        -- Centroid as INT32 scaled (x1e6 ~ 0.11m precision) for map pin
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
-- Step 8: Export region index
-- ----------------------------------------------------------

.print '>>> Step 8: Building region index...'

COPY (
    SELECT
        country,
        region,
        list(DISTINCT h3_res4 ORDER BY h3_res4) AS tiles,
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
-- Step 9: Export city index (per-country, Hive)
-- ----------------------------------------------------------
-- Depth-1 countries (FR, ES, NL, etc.) have no region in Overture,
-- so same-name cities in geographically distant areas (e.g. mainland
-- France vs overseas territories) would merge into one row with a
-- planet-spanning bbox. Fix: include LEFT(postcode, 3) in GROUP BY
-- as a geographic disambiguator, but ONLY when region IS NULL.
-- Countries with region data (US, CA, AU, etc.) already disambiguate
-- via the region column. Using 3 chars (not 2) because FR overseas
-- prefixes 971-988 all share "97" but span the globe.
-- The postcode prefix is NOT stored in the output, only used for grouping.

.print '>>> Step 9: Building city index (per-country)...'

COPY (
    SELECT
        country,
        region,
        city,
        list(DISTINCT h3_res4 ORDER BY h3_res4) AS tiles,
        sum(cnt)::INTEGER AS addr_count,
        -- Bbox as INT32 scaled (x1e6) for map zoom-to-city
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
-- Step 10: Export street index (per-country, Hive)
-- ----------------------------------------------------------
-- Per-country files: street_index/NL.parquet (~1 MB).
-- WASM reads only the country file, caches it in memory.
-- Used for forward geocoding: street name -> h3_res4 tile(s).

.print '>>> Step 10: Building street index (per-country)...'

COPY (
    SELECT
        country,
        street_lower,
        list(DISTINCT h3_res4 ORDER BY h3_res4) AS tiles,
        sum(cnt)::INTEGER AS addr_count,
        -- primary_city: city where this street has the most addresses
        arg_max(city, cnt) AS primary_city,
        -- Weighted centroid as INT32 scaled (x1e6) for map pin
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
-- Step 11: Export number index (per-country, Hive)
-- ----------------------------------------------------------
-- Per-country files: number_index/country=XX/data_0.parquet.
-- NOT bulk-loaded into WASM (unlike street/postcode indexes).
-- Queried on-demand via read_parquet() with HTTP range requests
-- when the user types a street + number ("ready" mode).
--
-- ROW_GROUP_SIZE 2000 gives narrow street_lower ranges per group.
-- DuckDB-WASM flow for "keizersgracht 18":
--   1. Fetch Parquet footer (~1 KB, cached after first query)
--   2. Row-group min/max pushdown on street_lower -> 1-2 groups
--   3. Fetch matching group(s) (~100-150 KB range request)
--   4. Filter numbers[] array client-side for prefix "18"

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

.print '>>> GEOCODER v4 BUILD COMPLETE'
