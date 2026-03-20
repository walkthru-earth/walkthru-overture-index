-- ============================================================
-- Walkthru Global Address Density Index — DuckDB 1.5
--
-- Source:  Overture Maps addresses (read direct from S3)
-- Grid:   H3 hexagonal cells, resolutions 1–10
--
-- Address density is one of the strongest urbanization proxies:
-- high density = dense urban; sparse = rural/undeveloped.
-- ============================================================

.print '>>> Step 1: Overture addresses → res 10...'

CREATE OR REPLACE TABLE h3_res10 AS
SELECT
    h3_latlng_to_cell(ST_Y(geometry), ST_X(geometry), 10) AS h3_index,
    count(*)::INTEGER AS address_count,
    count(DISTINCT postal_code) FILTER (postal_code IS NOT NULL)::INTEGER AS unique_postcodes,
FROM read_parquet(
    getvariable('overture_source') || '/theme=addresses/type=address/*',
    hive_partitioning=0
)
WHERE geometry IS NOT NULL
GROUP BY 1;

SELECT count(*) AS h3_cells_res10, sum(address_count) AS total_addresses FROM h3_res10;

.print '>>> Writing res 10...'
COPY (SELECT * FROM h3_res10 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=10/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);

CREATE OR REPLACE MACRO _addr_rollup(src_tbl, tgt_res) AS TABLE
SELECT
    h3_cell_to_parent(h3_index, tgt_res) AS h3_index,
    sum(address_count)::INTEGER AS address_count,
    sum(unique_postcodes)::INTEGER AS unique_postcodes
FROM src_tbl GROUP BY 1;

.print '>>> Rolling up res 9...'
CREATE OR REPLACE TABLE h3_res9 AS FROM _addr_rollup(h3_res10, 9);
COPY (SELECT * FROM h3_res9 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=9/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res10;

.print '>>> Rolling up res 8...'
CREATE OR REPLACE TABLE h3_res8 AS FROM _addr_rollup(h3_res9, 8);
COPY (SELECT * FROM h3_res8 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=8/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res9;

.print '>>> Rolling up res 7...'
CREATE OR REPLACE TABLE h3_res7 AS FROM _addr_rollup(h3_res8, 7);
COPY (SELECT * FROM h3_res7 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=7/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res8;

.print '>>> Rolling up res 6...'
CREATE OR REPLACE TABLE h3_res6 AS FROM _addr_rollup(h3_res7, 6);
COPY (SELECT * FROM h3_res6 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=6/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res7;

.print '>>> Rolling up res 5...'
CREATE OR REPLACE TABLE h3_res5 AS FROM _addr_rollup(h3_res6, 5);
COPY (SELECT * FROM h3_res5 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=5/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res6;

.print '>>> Rolling up res 4...'
CREATE OR REPLACE TABLE h3_res4 AS FROM _addr_rollup(h3_res5, 4);
COPY (SELECT * FROM h3_res4 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=4/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res5;

.print '>>> Rolling up res 3...'
CREATE OR REPLACE TABLE h3_res3 AS FROM _addr_rollup(h3_res4, 3);
COPY (SELECT * FROM h3_res3 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=3/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res4;

.print '>>> Rolling up res 2...'
CREATE OR REPLACE TABLE h3_res2 AS FROM _addr_rollup(h3_res3, 2);
COPY (SELECT * FROM h3_res2 ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=2/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res3;

.print '>>> Rolling up res 1...'
COPY (SELECT * FROM _addr_rollup(h3_res2, 1) ORDER BY h3_index) TO (getvariable('output_dir') || '/h3_res=1/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res2;

.print '>>> ALL 10 RESOLUTIONS COMPLETE'
