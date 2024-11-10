DROP TABLE IF EXISTS mapreduce_output;
DROP TABLE IF EXISTS datasource4;
DROP TABLE IF EXISTS state_producer_avg_price;
DROP TABLE IF EXISTS producers_by_state_3;
DROP TABLE IF EXISTS results;

CREATE EXTERNAL TABLE mapreduce_output (
    geo_id INT,
    producer STRING,
    car_count INT,
    total_price DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
location '${hivevar:input_dir3}';

-- Wyświetlenie zawartości tabeli mapreduce_output
SELECT * FROM mapreduce_output LIMIT 10;

CREATE EXTERNAL TABLE datasource4 (
    id INT,
    region STRING,
    region_url STRING,
    county STRING,
    state STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '^'
STORED AS TEXTFILE
location '${hivevar:input_dir4}';

-- Wyświetlenie zawartości tabeli datasource4
SELECT * FROM datasource4 LIMIT 10;

CREATE TABLE results (
        state STRING,
        manufacturer STRING,
        avg_price DOUBLE
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hivevar:output_dir6}';

WITH state_producer_avg_price AS (
    SELECT
        d.state,
        m.producer,
        SUM(m.total_price) / SUM(m.car_count) AS avg_price
    FROM mapreduce_output m
    JOIN datasource4 d ON m.geo_id = d.id
    GROUP BY
        d.state,
        m.producer
),
ranked_producers AS (
    SELECT
        state,
        producer,
        avg_price,
        ROW_NUMBER() OVER (PARTITION BY state ORDER BY avg_price DESC) AS rank
    FROM state_producer_avg_price
)
INSERT OVERWRITE TABLE results
SELECT
    state,
    producer,
    avg_price
FROM ranked_producers
WHERE rank <= 3;

