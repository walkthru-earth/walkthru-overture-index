-- ============================================================
-- Walkthru Global Transportation Index — DuckDB 1.5
--
-- Source:  Overture Maps (343M+ segments, read direct from S3)
-- Grid:   H3 hexagonal cells, resolutions 1–10
-- Schema: v1 (BIGINT h3_index, no geometry stored)
--
-- OPTIMIZATION STRATEGY:
--   1. Read Overture S3 once → compute h3 res 10 (finest)
--      using segment centroid for H3 assignment
--   2. Aggregate at res 10 (smallest groups, fast)
--   3. Derive res 1–9 by rolling up from res 10 via h3_cell_to_parent
--      (aggregate-of-aggregates — no re-scan needed)
--   4. Parquet v2 with BYTE_STREAM_SPLIT + DELTA_BINARY_PACKED
--   5. preserve_insertion_order=false to reduce memory
--   6. Only non-empty H3 cells (GROUP BY naturally excludes empties)
--
-- Variables (set by main.py via getvariable()):
--   overture_source  e.g. 's3://overturemaps-us-west-2/release/2026-03-18.0'
--   output_dir       e.g. 's3://bucket/prefix/transportation-index/v1/h3'
-- ============================================================

.print '>>> Step 1: Single-pass scan of Overture transportation segments → res 10...'

CREATE OR REPLACE TABLE h3_res10 AS
SELECT
    h3_latlng_to_cell(
        ST_Y(ST_Centroid(geometry)),
        ST_X(ST_Centroid(geometry)),
        10
    ) AS h3_index,

    -- Total segment count
    count(*)::INTEGER AS segment_count,

    -- By subtype (road / rail / water)
    count(*) FILTER (subtype = 'road')::INTEGER    AS n_road,
    count(*) FILTER (subtype = 'rail')::INTEGER    AS n_rail,
    count(*) FILTER (subtype = 'water')::INTEGER   AS n_water,

    -- By road class (18 classes from Overture schema)
    count(*) FILTER (class = 'motorway')::INTEGER      AS n_motorway,
    count(*) FILTER (class = 'trunk')::INTEGER          AS n_trunk,
    count(*) FILTER (class = 'primary')::INTEGER        AS n_primary,
    count(*) FILTER (class = 'secondary')::INTEGER      AS n_secondary,
    count(*) FILTER (class = 'tertiary')::INTEGER       AS n_tertiary,
    count(*) FILTER (class = 'residential')::INTEGER    AS n_residential,
    count(*) FILTER (class = 'living_street')::INTEGER  AS n_living_street,
    count(*) FILTER (class = 'unclassified')::INTEGER   AS n_unclassified,
    count(*) FILTER (class = 'service')::INTEGER        AS n_service,
    count(*) FILTER (class = 'pedestrian')::INTEGER     AS n_pedestrian,
    count(*) FILTER (class = 'footway')::INTEGER        AS n_footway,
    count(*) FILTER (class = 'steps')::INTEGER          AS n_steps,
    count(*) FILTER (class = 'path')::INTEGER           AS n_path,
    count(*) FILTER (class = 'track')::INTEGER          AS n_track,
    count(*) FILTER (class = 'cycleway')::INTEGER       AS n_cycleway,
    count(*) FILTER (class = 'bridleway')::INTEGER      AS n_bridleway,
    count(*) FILTER (class = 'unknown')::INTEGER        AS n_class_unknown,

    -- Road flags
    count(*) FILTER (
        subtype = 'road' AND road_flags::VARCHAR LIKE '%is_bridge%'
    )::INTEGER AS n_bridge,
    count(*) FILTER (
        subtype = 'road' AND road_flags::VARCHAR LIKE '%is_tunnel%'
    )::INTEGER AS n_tunnel,
    count(*) FILTER (
        subtype = 'road' AND road_flags::VARCHAR LIKE '%is_link%'
    )::INTEGER AS n_link,

    -- Road surface
    count(*) FILTER (
        subtype = 'road' AND road_surface[1].value = 'paved'
    )::INTEGER AS n_paved,
    count(*) FILTER (
        subtype = 'road' AND road_surface[1].value = 'unpaved'
    )::INTEGER AS n_unpaved,

FROM read_parquet(
    getvariable('overture_source') || '/theme=transportation/type=segment/*',
    hive_partitioning=0
)
WHERE geometry IS NOT NULL
GROUP BY 1;

SELECT count(*) AS h3_cells_res10, sum(segment_count) AS total_segments FROM h3_res10;

-- ============================================================
-- Step 2: Write res 10
-- ============================================================

.print '>>> Step 2: Writing res 10...'

COPY (
    SELECT * FROM h3_res10 ORDER BY h3_index
) TO (getvariable('output_dir') || '/h3_res=10/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
   ROW_GROUP_SIZE 1000000);

-- ============================================================
-- Step 3: Roll up res 9 → 1 using h3_cell_to_parent
-- All columns are INTEGER counts → simple SUM (additive).
-- Each level: CREATE TABLE → COPY → DROP (free memory).
-- ============================================================

-- ── Macro: rollup (avoids repeating 27 column names 9 times) ──

CREATE OR REPLACE MACRO _transport_rollup(src_tbl, tgt_res) AS TABLE
SELECT
    h3_cell_to_parent(h3_index, tgt_res) AS h3_index,
    sum(segment_count)::INTEGER AS segment_count,
    sum(n_road)::INTEGER AS n_road,
    sum(n_rail)::INTEGER AS n_rail,
    sum(n_water)::INTEGER AS n_water,
    sum(n_motorway)::INTEGER AS n_motorway,
    sum(n_trunk)::INTEGER AS n_trunk,
    sum(n_primary)::INTEGER AS n_primary,
    sum(n_secondary)::INTEGER AS n_secondary,
    sum(n_tertiary)::INTEGER AS n_tertiary,
    sum(n_residential)::INTEGER AS n_residential,
    sum(n_living_street)::INTEGER AS n_living_street,
    sum(n_unclassified)::INTEGER AS n_unclassified,
    sum(n_service)::INTEGER AS n_service,
    sum(n_pedestrian)::INTEGER AS n_pedestrian,
    sum(n_footway)::INTEGER AS n_footway,
    sum(n_steps)::INTEGER AS n_steps,
    sum(n_path)::INTEGER AS n_path,
    sum(n_track)::INTEGER AS n_track,
    sum(n_cycleway)::INTEGER AS n_cycleway,
    sum(n_bridleway)::INTEGER AS n_bridleway,
    sum(n_class_unknown)::INTEGER AS n_class_unknown,
    sum(n_bridge)::INTEGER AS n_bridge,
    sum(n_tunnel)::INTEGER AS n_tunnel,
    sum(n_link)::INTEGER AS n_link,
    sum(n_paved)::INTEGER AS n_paved,
    sum(n_unpaved)::INTEGER AS n_unpaved
FROM query_table(src_tbl)
GROUP BY 1;

-- Res 9
.print '>>> Rolling up res 9...'
CREATE OR REPLACE TABLE h3_res9 AS FROM _transport_rollup(h3_res10, 9);
COPY (SELECT * FROM h3_res9 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=9/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res10;

-- Res 8
.print '>>> Rolling up res 8...'
CREATE OR REPLACE TABLE h3_res8 AS FROM _transport_rollup(h3_res9, 8);
COPY (SELECT * FROM h3_res8 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=8/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res9;

-- Res 7
.print '>>> Rolling up res 7...'
CREATE OR REPLACE TABLE h3_res7 AS FROM _transport_rollup(h3_res8, 7);
COPY (SELECT * FROM h3_res7 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=7/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res8;

-- Res 6
.print '>>> Rolling up res 6...'
CREATE OR REPLACE TABLE h3_res6 AS FROM _transport_rollup(h3_res7, 6);
COPY (SELECT * FROM h3_res6 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=6/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res7;

-- Res 5
.print '>>> Rolling up res 5...'
CREATE OR REPLACE TABLE h3_res5 AS FROM _transport_rollup(h3_res6, 5);
COPY (SELECT * FROM h3_res5 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=5/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res6;

-- Res 4
.print '>>> Rolling up res 4...'
CREATE OR REPLACE TABLE h3_res4 AS FROM _transport_rollup(h3_res5, 4);
COPY (SELECT * FROM h3_res4 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=4/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res5;

-- Res 3
.print '>>> Rolling up res 3...'
CREATE OR REPLACE TABLE h3_res3 AS FROM _transport_rollup(h3_res4, 3);
COPY (SELECT * FROM h3_res3 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=3/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res4;

-- Res 2
.print '>>> Rolling up res 2...'
CREATE OR REPLACE TABLE h3_res2 AS FROM _transport_rollup(h3_res3, 2);
COPY (SELECT * FROM h3_res2 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=2/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res3;

-- Res 1
.print '>>> Rolling up res 1...'
COPY (
    SELECT * FROM _transport_rollup(h3_res2, 1) ORDER BY h3_index
) TO (getvariable('output_dir') || '/h3_res=1/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res2;

.print '>>> ALL 10 RESOLUTIONS COMPLETE'
