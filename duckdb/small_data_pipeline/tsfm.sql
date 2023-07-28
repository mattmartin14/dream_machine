
-- load base data
CREATE TABLE data_raw as
SELECT * FROM read_csv_auto('@source_file')
;

-- get total occurances of a full name and filter for top 10
CREATE TABLE data_agg
AS
SELECT full_nm
    ,rec_cnt
    ,DENSE_RANK() OVER(ORDER BY rec_cnt DESC) as ranked_cnt
FROM (
    SELECT 
    concat(first_name, ' ',last_name) as full_nm, count(*) as rec_cnt
    FROM data_raw
    GROUP BY 1
    --ORDER BY 2 DESC
) AS SUB
QUALIFY ranked_cnt BETWEEN 1 AND 5
;

-- write out to parquet file
COPY (SELECT * FROM data_agg) TO '@dest_file' (FORMAT PARQUET)
;

-- check parquet output
SELECT *
FROM read_parquet('@dest_file')
;