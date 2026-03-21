-- ============================================================
-- Walkthru Global Places Index — DuckDB 1.5
--
-- Source:  Overture Maps places (73M+ POIs, read direct from S3)
-- Grid:   H3 hexagonal cells, resolutions 1–10
-- Schema: v1 (BIGINT h3_index, no geometry stored)
--
-- Adapted from walkthru-places-index optimized build.
-- Variables set by main.py via getvariable().
-- ============================================================

.print '>>> Step 1: Single-pass scan → res 10 aggregation...'

CREATE OR REPLACE TABLE h3_res10 AS
SELECT
    h3_latlng_to_cell(ST_Y(geometry), ST_X(geometry), 10) AS h3_index,

    count(*)::INTEGER                                       AS place_count,
    sum(confidence)::FLOAT                                  AS confidence_sum,

    -- Top-level taxonomy (13 categories)
    count(*) FILTER (categories.primary = 'food_and_drink')::INTEGER           AS n_food_and_drink,
    count(*) FILTER (categories.primary = 'shopping')::INTEGER                 AS n_shopping,
    count(*) FILTER (categories.primary = 'services_and_business')::INTEGER    AS n_services_and_business,
    count(*) FILTER (categories.primary = 'health_care')::INTEGER              AS n_health_care,
    count(*) FILTER (categories.primary = 'travel_and_transportation')::INTEGER AS n_travel_and_transportation,
    count(*) FILTER (categories.primary = 'lifestyle_services')::INTEGER       AS n_lifestyle_services,
    count(*) FILTER (categories.primary = 'education')::INTEGER                AS n_education,
    count(*) FILTER (categories.primary = 'community_and_government')::INTEGER AS n_community_and_government,
    count(*) FILTER (categories.primary = 'cultural_and_historic')::INTEGER    AS n_cultural_and_historic,
    count(*) FILTER (categories.primary = 'sports_and_recreation')::INTEGER    AS n_sports_and_recreation,
    count(*) FILTER (categories.primary = 'lodging')::INTEGER                  AS n_lodging,
    count(*) FILTER (categories.primary = 'arts_and_entertainment')::INTEGER   AS n_arts_and_entertainment,
    count(*) FILTER (categories.primary = 'geographic_entities')::INTEGER      AS n_geographic_entities,

    -- Top subcategories (25 most common globally)
    count(*) FILTER (categories.primary = 'restaurant')::INTEGER               AS n_restaurant,
    count(*) FILTER (categories.primary = 'beauty_salon')::INTEGER             AS n_beauty_salon,
    count(*) FILTER (categories.primary = 'hotel')::INTEGER                    AS n_hotel,
    count(*) FILTER (categories.primary = 'grocery_store')::INTEGER            AS n_grocery_store,
    count(*) FILTER (categories.primary = 'cafe')::INTEGER                     AS n_cafe,
    count(*) FILTER (categories.primary = 'coffee_shop')::INTEGER              AS n_coffee_shop,
    count(*) FILTER (categories.primary = 'bar')::INTEGER                      AS n_bar,
    count(*) FILTER (categories.primary = 'pharmacy')::INTEGER                 AS n_pharmacy,
    count(*) FILTER (categories.primary = 'gas_station')::INTEGER              AS n_gas_station,
    count(*) FILTER (categories.primary = 'fast_food_restaurant')::INTEGER     AS n_fast_food_restaurant,
    count(*) FILTER (categories.primary = 'bakery')::INTEGER                   AS n_bakery,
    count(*) FILTER (categories.primary = 'gym_or_fitness_center')::INTEGER    AS n_gym_or_fitness_center,
    count(*) FILTER (categories.primary = 'park')::INTEGER                     AS n_park,
    count(*) FILTER (categories.primary = 'hospital')::INTEGER                 AS n_hospital,
    count(*) FILTER (categories.primary = 'school')::INTEGER                   AS n_school,

FROM read_parquet(
    getvariable('overture_source') || '/theme=places/type=place/*',
    hive_partitioning=0
)
WHERE geometry IS NOT NULL
GROUP BY 1;

SELECT count(*) AS h3_cells_res10, sum(place_count) AS total_places FROM h3_res10;

-- ============================================================
-- Step 2: Write res 10
-- ============================================================

.print '>>> Writing res 10...'

COPY (
    SELECT h3_index, place_count,
           (confidence_sum / place_count)::DECIMAL(4,3) AS avg_confidence,
           * EXCLUDE (h3_index, place_count, confidence_sum)
    FROM h3_res10 ORDER BY h3_index
) TO (getvariable('output_dir') || '/h3_res=10/data.parquet')
  (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
   ROW_GROUP_SIZE 1000000);

-- ============================================================
-- Step 3: Roll up 9 → 1 (SUM for counts, weighted avg for confidence)
-- ============================================================

CREATE OR REPLACE MACRO _places_rollup(src_tbl, tgt_res) AS TABLE
SELECT
    h3_cell_to_parent(h3_index, tgt_res) AS h3_index,
    sum(place_count)::INTEGER AS place_count,
    sum(confidence_sum) AS confidence_sum,
    sum(n_food_and_drink)::INTEGER AS n_food_and_drink,
    sum(n_shopping)::INTEGER AS n_shopping,
    sum(n_services_and_business)::INTEGER AS n_services_and_business,
    sum(n_health_care)::INTEGER AS n_health_care,
    sum(n_travel_and_transportation)::INTEGER AS n_travel_and_transportation,
    sum(n_lifestyle_services)::INTEGER AS n_lifestyle_services,
    sum(n_education)::INTEGER AS n_education,
    sum(n_community_and_government)::INTEGER AS n_community_and_government,
    sum(n_cultural_and_historic)::INTEGER AS n_cultural_and_historic,
    sum(n_sports_and_recreation)::INTEGER AS n_sports_and_recreation,
    sum(n_lodging)::INTEGER AS n_lodging,
    sum(n_arts_and_entertainment)::INTEGER AS n_arts_and_entertainment,
    sum(n_geographic_entities)::INTEGER AS n_geographic_entities,
    sum(n_restaurant)::INTEGER AS n_restaurant,
    sum(n_beauty_salon)::INTEGER AS n_beauty_salon,
    sum(n_hotel)::INTEGER AS n_hotel,
    sum(n_grocery_store)::INTEGER AS n_grocery_store,
    sum(n_cafe)::INTEGER AS n_cafe,
    sum(n_coffee_shop)::INTEGER AS n_coffee_shop,
    sum(n_bar)::INTEGER AS n_bar,
    sum(n_pharmacy)::INTEGER AS n_pharmacy,
    sum(n_gas_station)::INTEGER AS n_gas_station,
    sum(n_fast_food_restaurant)::INTEGER AS n_fast_food_restaurant,
    sum(n_bakery)::INTEGER AS n_bakery,
    sum(n_gym_or_fitness_center)::INTEGER AS n_gym_or_fitness_center,
    sum(n_park)::INTEGER AS n_park,
    sum(n_hospital)::INTEGER AS n_hospital,
    sum(n_school)::INTEGER AS n_school
FROM query_table(src_tbl) GROUP BY 1;

.print '>>> Rolling up res 9...'
CREATE OR REPLACE TABLE h3_res9 AS FROM _places_rollup(h3_res10, 9);
COPY (SELECT h3_index, place_count, (confidence_sum/place_count)::DECIMAL(4,3) AS avg_confidence, * EXCLUDE (h3_index, place_count, confidence_sum) FROM h3_res9 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=9/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res10;

.print '>>> Rolling up res 8...'
CREATE OR REPLACE TABLE h3_res8 AS FROM _places_rollup(h3_res9, 8);
COPY (SELECT h3_index, place_count, (confidence_sum/place_count)::DECIMAL(4,3) AS avg_confidence, * EXCLUDE (h3_index, place_count, confidence_sum) FROM h3_res8 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=8/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res9;

.print '>>> Rolling up res 7...'
CREATE OR REPLACE TABLE h3_res7 AS FROM _places_rollup(h3_res8, 7);
COPY (SELECT h3_index, place_count, (confidence_sum/place_count)::DECIMAL(4,3) AS avg_confidence, * EXCLUDE (h3_index, place_count, confidence_sum) FROM h3_res7 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=7/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res8;

.print '>>> Rolling up res 6...'
CREATE OR REPLACE TABLE h3_res6 AS FROM _places_rollup(h3_res7, 6);
COPY (SELECT h3_index, place_count, (confidence_sum/place_count)::DECIMAL(4,3) AS avg_confidence, * EXCLUDE (h3_index, place_count, confidence_sum) FROM h3_res6 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=6/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res7;

.print '>>> Rolling up res 5...'
CREATE OR REPLACE TABLE h3_res5 AS FROM _places_rollup(h3_res6, 5);
COPY (SELECT h3_index, place_count, (confidence_sum/place_count)::DECIMAL(4,3) AS avg_confidence, * EXCLUDE (h3_index, place_count, confidence_sum) FROM h3_res5 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=5/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res6;

.print '>>> Rolling up res 4...'
CREATE OR REPLACE TABLE h3_res4 AS FROM _places_rollup(h3_res5, 4);
COPY (SELECT h3_index, place_count, (confidence_sum/place_count)::DECIMAL(4,3) AS avg_confidence, * EXCLUDE (h3_index, place_count, confidence_sum) FROM h3_res4 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=4/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res5;

.print '>>> Rolling up res 3...'
CREATE OR REPLACE TABLE h3_res3 AS FROM _places_rollup(h3_res4, 3);
COPY (SELECT h3_index, place_count, (confidence_sum/place_count)::DECIMAL(4,3) AS avg_confidence, * EXCLUDE (h3_index, place_count, confidence_sum) FROM h3_res3 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=3/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res4;

.print '>>> Rolling up res 2...'
CREATE OR REPLACE TABLE h3_res2 AS FROM _places_rollup(h3_res3, 2);
COPY (SELECT h3_index, place_count, (confidence_sum/place_count)::DECIMAL(4,3) AS avg_confidence, * EXCLUDE (h3_index, place_count, confidence_sum) FROM h3_res2 ORDER BY h3_index)
  TO (getvariable('output_dir') || '/h3_res=2/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res3;

.print '>>> Rolling up res 1...'
COPY (SELECT h3_index, sum(place_count)::INTEGER AS place_count, (sum(confidence_sum)/sum(place_count))::DECIMAL(4,3) AS avg_confidence,
    sum(n_food_and_drink)::INTEGER AS n_food_and_drink, sum(n_shopping)::INTEGER AS n_shopping,
    sum(n_services_and_business)::INTEGER AS n_services_and_business, sum(n_health_care)::INTEGER AS n_health_care,
    sum(n_travel_and_transportation)::INTEGER AS n_travel_and_transportation, sum(n_lifestyle_services)::INTEGER AS n_lifestyle_services,
    sum(n_education)::INTEGER AS n_education, sum(n_community_and_government)::INTEGER AS n_community_and_government,
    sum(n_cultural_and_historic)::INTEGER AS n_cultural_and_historic, sum(n_sports_and_recreation)::INTEGER AS n_sports_and_recreation,
    sum(n_lodging)::INTEGER AS n_lodging, sum(n_arts_and_entertainment)::INTEGER AS n_arts_and_entertainment,
    sum(n_geographic_entities)::INTEGER AS n_geographic_entities,
    sum(n_restaurant)::INTEGER AS n_restaurant, sum(n_beauty_salon)::INTEGER AS n_beauty_salon,
    sum(n_hotel)::INTEGER AS n_hotel, sum(n_grocery_store)::INTEGER AS n_grocery_store,
    sum(n_cafe)::INTEGER AS n_cafe, sum(n_coffee_shop)::INTEGER AS n_coffee_shop,
    sum(n_bar)::INTEGER AS n_bar, sum(n_pharmacy)::INTEGER AS n_pharmacy,
    sum(n_gas_station)::INTEGER AS n_gas_station, sum(n_fast_food_restaurant)::INTEGER AS n_fast_food_restaurant,
    sum(n_bakery)::INTEGER AS n_bakery, sum(n_gym_or_fitness_center)::INTEGER AS n_gym_or_fitness_center,
    sum(n_park)::INTEGER AS n_park, sum(n_hospital)::INTEGER AS n_hospital, sum(n_school)::INTEGER AS n_school
  FROM h3_res2 GROUP BY h3_cell_to_parent(h3_index, 1) ORDER BY 1
) TO (getvariable('output_dir') || '/h3_res=1/data.parquet') (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE 1000000);
DROP TABLE h3_res2;

.print '>>> ALL 10 RESOLUTIONS COMPLETE'
