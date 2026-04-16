-- ============================================================
-- Walkthru Global Building Classification Index — DuckDB 1.5
--
-- Source:  Overture Maps buildings (2.3B+ buildings, read direct from S3)
-- Grid:   H3 hexagonal cells, resolutions 1–10
--
-- COMPLEMENTS the existing walkthru-building-index (GBA) which has
-- height/footprint/volume metrics. This index adds building USE/CLASS
-- breakdown from Overture's taxonomy — essential for urban land use
-- analysis (residential vs commercial vs industrial density).
-- ============================================================

.print '>>> Step 1: Overture buildings → res 10 classification index...'

CREATE OR REPLACE TABLE h3_res10 AS
SELECT
    h3_latlng_to_cell(ST_Y(ST_Centroid(geometry)), ST_X(ST_Centroid(geometry)), 10) AS h3_index,

    count(*)::INTEGER AS building_count,

    -- By subtype (13 Overture building subtypes)
    count(*) FILTER (subtype = 'residential')::INTEGER      AS n_residential,
    count(*) FILTER (subtype = 'commercial')::INTEGER        AS n_commercial,
    count(*) FILTER (subtype = 'industrial')::INTEGER        AS n_industrial,
    count(*) FILTER (subtype = 'civic')::INTEGER             AS n_civic,
    count(*) FILTER (subtype = 'education')::INTEGER         AS n_education,
    count(*) FILTER (subtype = 'medical')::INTEGER           AS n_medical,
    count(*) FILTER (subtype = 'religious')::INTEGER         AS n_religious,
    count(*) FILTER (subtype = 'entertainment')::INTEGER     AS n_entertainment,
    count(*) FILTER (subtype = 'military')::INTEGER          AS n_military,
    count(*) FILTER (subtype = 'agricultural')::INTEGER      AS n_agricultural,
    count(*) FILTER (subtype = 'service')::INTEGER           AS n_service,
    count(*) FILTER (subtype = 'transportation')::INTEGER    AS n_transportation,
    count(*) FILTER (subtype = 'outbuilding')::INTEGER       AS n_outbuilding,

    -- Top detailed classes (most common globally)
    count(*) FILTER (class = 'house')::INTEGER               AS n_house,
    count(*) FILTER (class = 'apartments')::INTEGER          AS n_apartments,
    count(*) FILTER (class = 'detached')::INTEGER            AS n_detached,
    count(*) FILTER (class = 'commercial')::INTEGER          AS n_class_commercial,
    count(*) FILTER (class = 'industrial')::INTEGER          AS n_class_industrial,
    count(*) FILTER (class = 'retail')::INTEGER              AS n_retail,
    count(*) FILTER (class = 'warehouse')::INTEGER           AS n_warehouse,
    count(*) FILTER (class = 'office')::INTEGER              AS n_office,
    count(*) FILTER (class = 'school')::INTEGER              AS n_class_school,
    count(*) FILTER (class = 'university')::INTEGER          AS n_university,
    count(*) FILTER (class = 'hospital')::INTEGER            AS n_class_hospital,
    count(*) FILTER (class = 'church')::INTEGER              AS n_church,
    count(*) FILTER (class = 'mosque')::INTEGER              AS n_mosque,
    count(*) FILTER (class = 'hotel')::INTEGER               AS n_class_hotel,
    count(*) FILTER (class = 'garage')::INTEGER              AS n_garage,
    count(*) FILTER (class = 'farm')::INTEGER                AS n_farm,
    count(*) FILTER (class = 'barn')::INTEGER                AS n_barn,
    count(*) FILTER (class = 'factory')::INTEGER             AS n_factory,
    count(*) FILTER (class = 'greenhouse')::INTEGER          AS n_greenhouse,
    count(*) FILTER (class = 'hangar')::INTEGER              AS n_hangar,

    -- Has parts flag (complex buildings)
    count(*) FILTER (has_parts = true)::INTEGER              AS n_has_parts,

    -- Height stats (Overture provides height for some buildings)
    count(*) FILTER (height IS NOT NULL)::INTEGER            AS n_with_height,
    avg(height) FILTER (height IS NOT NULL)::FLOAT           AS avg_height_m,
    max(height) FILTER (height IS NOT NULL)::FLOAT           AS max_height_m,

    -- Floor stats
    count(*) FILTER (num_floors IS NOT NULL)::INTEGER        AS n_with_floors,
    avg(num_floors) FILTER (num_floors IS NOT NULL)::FLOAT   AS avg_floors,
    max(num_floors) FILTER (num_floors IS NOT NULL)::INTEGER AS max_floors,

FROM read_parquet(
    getvariable('overture_source') || '/theme=buildings/type=building/*',
    hive_partitioning=0
)
WHERE geometry IS NOT NULL
GROUP BY 1;

SELECT count(*) AS h3_cells_res10, sum(building_count) AS total_buildings FROM h3_res10;

.print '>>> Writing res 10...'
COPY (SELECT * FROM h3_res10 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=10/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);

-- ============================================================
-- Rollup macro — counts SUM, height/floors need weighted avg
-- ============================================================

CREATE OR REPLACE MACRO _buildings_rollup(src_tbl, tgt_res) AS TABLE
SELECT
    h3_cell_to_parent(h3_index, tgt_res) AS h3_index,
    sum(building_count)::INTEGER AS building_count,
    sum(n_residential)::INTEGER AS n_residential, sum(n_commercial)::INTEGER AS n_commercial,
    sum(n_industrial)::INTEGER AS n_industrial, sum(n_civic)::INTEGER AS n_civic,
    sum(n_education)::INTEGER AS n_education, sum(n_medical)::INTEGER AS n_medical,
    sum(n_religious)::INTEGER AS n_religious, sum(n_entertainment)::INTEGER AS n_entertainment,
    sum(n_military)::INTEGER AS n_military, sum(n_agricultural)::INTEGER AS n_agricultural,
    sum(n_service)::INTEGER AS n_service, sum(n_transportation)::INTEGER AS n_transportation,
    sum(n_outbuilding)::INTEGER AS n_outbuilding,
    sum(n_house)::INTEGER AS n_house, sum(n_apartments)::INTEGER AS n_apartments,
    sum(n_detached)::INTEGER AS n_detached, sum(n_class_commercial)::INTEGER AS n_class_commercial,
    sum(n_class_industrial)::INTEGER AS n_class_industrial, sum(n_retail)::INTEGER AS n_retail,
    sum(n_warehouse)::INTEGER AS n_warehouse, sum(n_office)::INTEGER AS n_office,
    sum(n_class_school)::INTEGER AS n_class_school, sum(n_university)::INTEGER AS n_university,
    sum(n_class_hospital)::INTEGER AS n_class_hospital, sum(n_church)::INTEGER AS n_church,
    sum(n_mosque)::INTEGER AS n_mosque, sum(n_class_hotel)::INTEGER AS n_class_hotel,
    sum(n_garage)::INTEGER AS n_garage, sum(n_farm)::INTEGER AS n_farm,
    sum(n_barn)::INTEGER AS n_barn, sum(n_factory)::INTEGER AS n_factory,
    sum(n_greenhouse)::INTEGER AS n_greenhouse, sum(n_hangar)::INTEGER AS n_hangar,
    sum(n_has_parts)::INTEGER AS n_has_parts,
    sum(n_with_height)::INTEGER AS n_with_height,
    -- Weighted average height: sum(avg*count) / sum(count)
    CASE WHEN sum(n_with_height) > 0
         THEN (sum(avg_height_m * n_with_height) / sum(n_with_height))::FLOAT
         ELSE NULL END AS avg_height_m,
    max(max_height_m)::FLOAT AS max_height_m,
    sum(n_with_floors)::INTEGER AS n_with_floors,
    CASE WHEN sum(n_with_floors) > 0
         THEN (sum(avg_floors * n_with_floors) / sum(n_with_floors))::FLOAT
         ELSE NULL END AS avg_floors,
    max(max_floors)::INTEGER AS max_floors
FROM query_table(src_tbl) GROUP BY 1;

.print '>>> Rolling up res 9...'
CREATE OR REPLACE TABLE h3_res9 AS FROM _buildings_rollup(h3_res10, 9);
COPY (SELECT * FROM h3_res9 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=9/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res10;

.print '>>> Rolling up res 8...'
CREATE OR REPLACE TABLE h3_res8 AS FROM _buildings_rollup(h3_res9, 8);
COPY (SELECT * FROM h3_res8 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=8/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res9;

.print '>>> Rolling up res 7...'
CREATE OR REPLACE TABLE h3_res7 AS FROM _buildings_rollup(h3_res8, 7);
COPY (SELECT * FROM h3_res7 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=7/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res8;

.print '>>> Rolling up res 6...'
CREATE OR REPLACE TABLE h3_res6 AS FROM _buildings_rollup(h3_res7, 6);
COPY (SELECT * FROM h3_res6 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=6/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res7;

.print '>>> Rolling up res 5...'
CREATE OR REPLACE TABLE h3_res5 AS FROM _buildings_rollup(h3_res6, 5);
COPY (SELECT * FROM h3_res5 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=5/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res6;

.print '>>> Rolling up res 4...'
CREATE OR REPLACE TABLE h3_res4 AS FROM _buildings_rollup(h3_res5, 4);
COPY (SELECT * FROM h3_res4 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=4/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res5;

.print '>>> Rolling up res 3...'
CREATE OR REPLACE TABLE h3_res3 AS FROM _buildings_rollup(h3_res4, 3);
COPY (SELECT * FROM h3_res3 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=3/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res4;

.print '>>> Rolling up res 2...'
CREATE OR REPLACE TABLE h3_res2 AS FROM _buildings_rollup(h3_res3, 2);
COPY (SELECT * FROM h3_res2 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=2/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res3;

.print '>>> Rolling up res 1...'
COPY (SELECT * FROM _buildings_rollup(h3_res2, 1) ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=1/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res2;

.print '>>> ALL 10 RESOLUTIONS COMPLETE'
