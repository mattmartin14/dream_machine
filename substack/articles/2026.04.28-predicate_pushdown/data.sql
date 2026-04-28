SET VARIABLE min_val = 5;
SET VARIABLE max_val = 1_000_000;
SET VARIABLE min_date_grp_1 = DATE '2025-01-01';
SET VARIABLE max_date_grp_1 = DATE '2025-12-31';
SET VARIABLE min_date_grp_2 = DATE '2026-01-01';
SET VARIABLE max_date_grp_2 = DATE '2026-12-31';
SET VARIABLE row_cnt = 10_000_000;

CREATE OR REPLACE VIEW V_DATA AS
SELECT id
        ,floor(random() * (getvariable('max_val') - getvariable('min_val') + 1) + getvariable('min_val'))::int as val
        ,(getvariable('min_date_grp_1') + (random() * (
            getvariable('max_date_grp_1') - getvariable('min_date_grp_1')
        ))::INT)::DATE AS random_date
    FROM RANGE(0,getvariable('row_cnt')) as t(id)
    UNION ALL
    SELECT id
        ,floor(random() * (getvariable('max_val') - getvariable('min_val') + 1) + getvariable('min_val'))::int as val
        ,(getvariable('min_date_grp_2') + (random() * (
            getvariable('max_date_grp_2') - getvariable('min_date_grp_2')
        ))::INT)::DATE AS random_date
    FROM RANGE(0,getvariable('row_cnt')) as t(id)
;

COPY (
    FROM V_DATA
    ORDER BY random_date
) to 'data_date_sorted.parquet'
;

COPY (
    FROM V_DATA
    ORDER BY id
) to 'data_id_sorted.parquet'
;

-- file sizes
SELECT
    'data_date_sorted.parquet' AS file_name,
    size / 1024.0 / 1024.0 AS size_mb
FROM read_blob('data_date_sorted.parquet')
UNION ALL
SELECT
    'data_id_sorted.parquet' AS file_name,
    size / 1024.0 / 1024.0 AS size_mb
FROM read_blob('data_id_sorted.parquet')
;

PRAGMA explain_output = 'all';

.print 'sample metadata from date-sorted parquet'
SELECT row_group_id, path_in_schema as column_name, type
    ,stats_min, stats_max
FROM parquet_metadata('data_date_sorted.parquet')
ORDER BY row_group_id, column_name
lIMIT 10
;

.print 'getting count of row groups from date sourted parquet file'
select count(distinct row_group_id) as total_row_groups
from parquet_metadata('data_date_sorted.parquet')
;

.print 'getting count of row groups that may be scanned for date-sorted parquet'
select count(distinct row_group_id) as row_groups_may_scan_date_sorted
from parquet_metadata('data_date_sorted.parquet')
WHERE path_in_schema = 'random_date'
    and not (
        try_cast(stats_max as date) < date '2025-01-01'
        or try_cast(stats_min as date) > date '2025-01-23'
    )
;

select count(distinct row_group_id) as row_groups_may_scan_id_sorted
from parquet_metadata('data_id_sorted.parquet')
WHERE path_in_schema = 'random_date'
    and not (
        try_cast(stats_max as date) < date '2025-01-01'
        or try_cast(stats_min as date) > date '2025-01-23'
    )
;

-- predicate push down of the id
EXPLAIN ANALYZE
SELECT sum(val), count(distinct id) as id_cnt
from 'data_date_sorted.parquet'
WHERE random_date between '2025-01-01' and '2025-01-23'
;

EXPLAIN ANALYZE
SELECT sum(val), count(distinct id) as id_cnt
from 'data_id_sorted.parquet'
WHERE random_date between '2025-01-01' and '2025-01-23'
;