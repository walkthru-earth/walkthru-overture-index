-- ============================================================
-- Walkthru Global Base Environment Index — DuckDB 1.5
--
-- Source:  Overture Maps base theme (infrastructure + land_use + water)
-- Grid:   H3 hexagonal cells, resolutions 1–10
--
-- Combines three base sub-themes into one environment index:
--   - Infrastructure: power, telecom, transit, bridges, piers
--   - Land use: residential/commercial/industrial zones, parks, farms
--   - Water: rivers, lakes, oceans, canals
--
-- Uses polygon CENTROID for H3 assignment (adequate for aggregated counts).
-- ============================================================

-- ── Infrastructure ──────────────────────────────────────────

.print '>>> Scanning Overture base/infrastructure → res 10...'

CREATE OR REPLACE TABLE infra_res10 AS
SELECT
    h3_latlng_to_cell(ST_Y(ST_Centroid(geometry)), ST_X(ST_Centroid(geometry)), 10) AS h3_index,
    count(*)::INTEGER AS infra_count,
    count(*) FILTER (subtype = 'power')::INTEGER           AS n_power,
    count(*) FILTER (subtype = 'communication')::INTEGER   AS n_communication,
    count(*) FILTER (subtype = 'transit')::INTEGER         AS n_transit,
    count(*) FILTER (subtype = 'bridge')::INTEGER          AS n_bridge,
    count(*) FILTER (subtype = 'tower')::INTEGER           AS n_tower,
    count(*) FILTER (subtype = 'pier')::INTEGER            AS n_pier,
    count(*) FILTER (subtype = 'barrier')::INTEGER         AS n_barrier,
    count(*) FILTER (subtype = 'water')::INTEGER           AS n_water_infra,
    count(*) FILTER (subtype = 'utility')::INTEGER         AS n_utility,
    count(*) FILTER (subtype = 'airport')::INTEGER         AS n_airport,
    count(*) FILTER (subtype = 'recreation')::INTEGER      AS n_recreation,
    count(*) FILTER (subtype = 'waste_management')::INTEGER AS n_waste_mgmt,
FROM read_parquet(
    getvariable('overture_source') || '/theme=base/type=infrastructure/*',
    hive_partitioning=0
)
WHERE geometry IS NOT NULL
GROUP BY 1;

-- ── Land use ────────────────────────────────────────────────

.print '>>> Scanning Overture base/land_use → res 10...'

CREATE OR REPLACE TABLE landuse_res10 AS
SELECT
    h3_latlng_to_cell(ST_Y(ST_Centroid(geometry)), ST_X(ST_Centroid(geometry)), 10) AS h3_index,
    count(*)::INTEGER AS landuse_count,
    count(*) FILTER (subtype = 'residential')::INTEGER         AS n_lu_residential,
    count(*) FILTER (subtype = 'developed')::INTEGER           AS n_lu_developed,
    count(*) FILTER (subtype = 'agriculture')::INTEGER         AS n_lu_agriculture,
    count(*) FILTER (subtype = 'park')::INTEGER                AS n_lu_park,
    count(*) FILTER (subtype = 'recreation')::INTEGER          AS n_lu_recreation,
    count(*) FILTER (subtype = 'education')::INTEGER           AS n_lu_education,
    count(*) FILTER (subtype = 'medical')::INTEGER             AS n_lu_medical,
    count(*) FILTER (subtype = 'military')::INTEGER            AS n_lu_military,
    count(*) FILTER (subtype = 'religious')::INTEGER           AS n_lu_religious,
    count(*) FILTER (subtype = 'cemetery')::INTEGER            AS n_lu_cemetery,
    count(*) FILTER (subtype = 'transportation')::INTEGER      AS n_lu_transportation,
    count(*) FILTER (subtype = 'entertainment')::INTEGER       AS n_lu_entertainment,
    count(*) FILTER (subtype = 'managed')::INTEGER             AS n_lu_managed,
    count(*) FILTER (subtype = 'protected')::INTEGER           AS n_lu_protected,
    count(*) FILTER (subtype = 'construction')::INTEGER        AS n_lu_construction,
    count(*) FILTER (subtype = 'resource_extraction')::INTEGER AS n_lu_resource,
FROM read_parquet(
    getvariable('overture_source') || '/theme=base/type=land_use/*',
    hive_partitioning=0
)
WHERE geometry IS NOT NULL
GROUP BY 1;

-- ── Water ───────────────────────────────────────────────────

.print '>>> Scanning Overture base/water → res 10...'

CREATE OR REPLACE TABLE water_res10 AS
SELECT
    h3_latlng_to_cell(ST_Y(ST_Centroid(geometry)), ST_X(ST_Centroid(geometry)), 10) AS h3_index,
    count(*)::INTEGER AS water_count,
    count(*) FILTER (subtype = 'river')::INTEGER       AS n_river,
    count(*) FILTER (subtype = 'lake')::INTEGER        AS n_lake,
    count(*) FILTER (subtype = 'ocean')::INTEGER       AS n_ocean,
    count(*) FILTER (subtype = 'stream')::INTEGER      AS n_stream,
    count(*) FILTER (subtype = 'canal')::INTEGER       AS n_canal,
    count(*) FILTER (subtype = 'pond')::INTEGER        AS n_pond,
    count(*) FILTER (subtype = 'reservoir')::INTEGER   AS n_reservoir,
    count(*) FILTER (subtype = 'spring')::INTEGER      AS n_spring,
    count(*) FILTER (is_salt = true)::INTEGER          AS n_salt_water,
    count(*) FILTER (is_intermittent = true)::INTEGER  AS n_intermittent,
FROM read_parquet(
    getvariable('overture_source') || '/theme=base/type=water/*',
    hive_partitioning=0
)
WHERE geometry IS NOT NULL
GROUP BY 1;

-- ── Merge all three into one environment table ──────────────

.print '>>> Merging infrastructure + land_use + water → unified base index...'

CREATE OR REPLACE TABLE h3_res10 AS
SELECT
    COALESCE(i.h3_index, l.h3_index, w.h3_index) AS h3_index,
    -- Infrastructure
    COALESCE(i.infra_count, 0)::INTEGER AS infra_count,
    COALESCE(i.n_power, 0)::INTEGER AS n_power,
    COALESCE(i.n_communication, 0)::INTEGER AS n_communication,
    COALESCE(i.n_transit, 0)::INTEGER AS n_transit,
    COALESCE(i.n_bridge, 0)::INTEGER AS n_bridge,
    COALESCE(i.n_tower, 0)::INTEGER AS n_tower,
    COALESCE(i.n_pier, 0)::INTEGER AS n_pier,
    COALESCE(i.n_barrier, 0)::INTEGER AS n_barrier,
    COALESCE(i.n_water_infra, 0)::INTEGER AS n_water_infra,
    COALESCE(i.n_utility, 0)::INTEGER AS n_utility,
    COALESCE(i.n_airport, 0)::INTEGER AS n_airport,
    COALESCE(i.n_recreation, 0)::INTEGER AS n_recreation,
    COALESCE(i.n_waste_mgmt, 0)::INTEGER AS n_waste_mgmt,
    -- Land use
    COALESCE(l.landuse_count, 0)::INTEGER AS landuse_count,
    COALESCE(l.n_lu_residential, 0)::INTEGER AS n_lu_residential,
    COALESCE(l.n_lu_developed, 0)::INTEGER AS n_lu_developed,
    COALESCE(l.n_lu_agriculture, 0)::INTEGER AS n_lu_agriculture,
    COALESCE(l.n_lu_park, 0)::INTEGER AS n_lu_park,
    COALESCE(l.n_lu_recreation, 0)::INTEGER AS n_lu_recreation,
    COALESCE(l.n_lu_education, 0)::INTEGER AS n_lu_education,
    COALESCE(l.n_lu_medical, 0)::INTEGER AS n_lu_medical,
    COALESCE(l.n_lu_military, 0)::INTEGER AS n_lu_military,
    COALESCE(l.n_lu_religious, 0)::INTEGER AS n_lu_religious,
    COALESCE(l.n_lu_cemetery, 0)::INTEGER AS n_lu_cemetery,
    COALESCE(l.n_lu_transportation, 0)::INTEGER AS n_lu_transportation,
    COALESCE(l.n_lu_entertainment, 0)::INTEGER AS n_lu_entertainment,
    COALESCE(l.n_lu_managed, 0)::INTEGER AS n_lu_managed,
    COALESCE(l.n_lu_protected, 0)::INTEGER AS n_lu_protected,
    COALESCE(l.n_lu_construction, 0)::INTEGER AS n_lu_construction,
    COALESCE(l.n_lu_resource, 0)::INTEGER AS n_lu_resource,
    -- Water
    COALESCE(w.water_count, 0)::INTEGER AS water_count,
    COALESCE(w.n_river, 0)::INTEGER AS n_river,
    COALESCE(w.n_lake, 0)::INTEGER AS n_lake,
    COALESCE(w.n_ocean, 0)::INTEGER AS n_ocean,
    COALESCE(w.n_stream, 0)::INTEGER AS n_stream,
    COALESCE(w.n_canal, 0)::INTEGER AS n_canal,
    COALESCE(w.n_pond, 0)::INTEGER AS n_pond,
    COALESCE(w.n_reservoir, 0)::INTEGER AS n_reservoir,
    COALESCE(w.n_spring, 0)::INTEGER AS n_spring,
    COALESCE(w.n_salt_water, 0)::INTEGER AS n_salt_water,
    COALESCE(w.n_intermittent, 0)::INTEGER AS n_intermittent,
FROM infra_res10 i
FULL OUTER JOIN landuse_res10 l ON i.h3_index = l.h3_index
FULL OUTER JOIN water_res10 w ON COALESCE(i.h3_index, l.h3_index) = w.h3_index;

DROP TABLE infra_res10;
DROP TABLE landuse_res10;
DROP TABLE water_res10;

SELECT count(*) AS h3_cells_res10 FROM h3_res10;

.print '>>> Writing res 10...'
COPY (SELECT * FROM h3_res10 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=10/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);

-- ── Rollup 9→1 (all columns are additive INTEGER counts) ───

CREATE OR REPLACE MACRO _base_rollup(src_tbl, tgt_res) AS TABLE
SELECT
    h3_cell_to_parent(h3_index, tgt_res) AS h3_index,
    sum(infra_count)::INTEGER AS infra_count,
    sum(n_power)::INTEGER AS n_power, sum(n_communication)::INTEGER AS n_communication,
    sum(n_transit)::INTEGER AS n_transit, sum(n_bridge)::INTEGER AS n_bridge,
    sum(n_tower)::INTEGER AS n_tower, sum(n_pier)::INTEGER AS n_pier,
    sum(n_barrier)::INTEGER AS n_barrier, sum(n_water_infra)::INTEGER AS n_water_infra,
    sum(n_utility)::INTEGER AS n_utility, sum(n_airport)::INTEGER AS n_airport,
    sum(n_recreation)::INTEGER AS n_recreation, sum(n_waste_mgmt)::INTEGER AS n_waste_mgmt,
    sum(landuse_count)::INTEGER AS landuse_count,
    sum(n_lu_residential)::INTEGER AS n_lu_residential, sum(n_lu_developed)::INTEGER AS n_lu_developed,
    sum(n_lu_agriculture)::INTEGER AS n_lu_agriculture, sum(n_lu_park)::INTEGER AS n_lu_park,
    sum(n_lu_recreation)::INTEGER AS n_lu_recreation, sum(n_lu_education)::INTEGER AS n_lu_education,
    sum(n_lu_medical)::INTEGER AS n_lu_medical, sum(n_lu_military)::INTEGER AS n_lu_military,
    sum(n_lu_religious)::INTEGER AS n_lu_religious, sum(n_lu_cemetery)::INTEGER AS n_lu_cemetery,
    sum(n_lu_transportation)::INTEGER AS n_lu_transportation, sum(n_lu_entertainment)::INTEGER AS n_lu_entertainment,
    sum(n_lu_managed)::INTEGER AS n_lu_managed, sum(n_lu_protected)::INTEGER AS n_lu_protected,
    sum(n_lu_construction)::INTEGER AS n_lu_construction, sum(n_lu_resource)::INTEGER AS n_lu_resource,
    sum(water_count)::INTEGER AS water_count,
    sum(n_river)::INTEGER AS n_river, sum(n_lake)::INTEGER AS n_lake,
    sum(n_ocean)::INTEGER AS n_ocean, sum(n_stream)::INTEGER AS n_stream,
    sum(n_canal)::INTEGER AS n_canal, sum(n_pond)::INTEGER AS n_pond,
    sum(n_reservoir)::INTEGER AS n_reservoir, sum(n_spring)::INTEGER AS n_spring,
    sum(n_salt_water)::INTEGER AS n_salt_water, sum(n_intermittent)::INTEGER AS n_intermittent
FROM query_table(src_tbl) GROUP BY 1;

.print '>>> Rolling up res 9...'
CREATE OR REPLACE TABLE h3_res9 AS FROM _base_rollup(h3_res10, 9);
COPY (SELECT * FROM h3_res9 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=9/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res10;

.print '>>> Rolling up res 8...'
CREATE OR REPLACE TABLE h3_res8 AS FROM _base_rollup(h3_res9, 8);
COPY (SELECT * FROM h3_res8 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=8/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res9;

.print '>>> Rolling up res 7...'
CREATE OR REPLACE TABLE h3_res7 AS FROM _base_rollup(h3_res8, 7);
COPY (SELECT * FROM h3_res7 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=7/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res8;

.print '>>> Rolling up res 6...'
CREATE OR REPLACE TABLE h3_res6 AS FROM _base_rollup(h3_res7, 6);
COPY (SELECT * FROM h3_res6 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=6/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res7;

.print '>>> Rolling up res 5...'
CREATE OR REPLACE TABLE h3_res5 AS FROM _base_rollup(h3_res6, 5);
COPY (SELECT * FROM h3_res5 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=5/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res6;

.print '>>> Rolling up res 4...'
CREATE OR REPLACE TABLE h3_res4 AS FROM _base_rollup(h3_res5, 4);
COPY (SELECT * FROM h3_res4 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=4/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res5;

.print '>>> Rolling up res 3...'
CREATE OR REPLACE TABLE h3_res3 AS FROM _base_rollup(h3_res4, 3);
COPY (SELECT * FROM h3_res3 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=3/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res4;

.print '>>> Rolling up res 2...'
CREATE OR REPLACE TABLE h3_res2 AS FROM _base_rollup(h3_res3, 2);
COPY (SELECT * FROM h3_res2 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=2/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res3;

.print '>>> Rolling up res 1...'
COPY (SELECT * FROM _base_rollup(h3_res2, 1) ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=1/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res2;

.print '>>> ALL 10 RESOLUTIONS COMPLETE'
