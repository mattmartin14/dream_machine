select
    stroke_number,
    total_time,
    total_distance,
    pace_seconds,
    pace_split_m,
    watts,
    stroke_rate,
    season_year,
    workout_date,
    machine_type,
    strftime(
        TIMESTAMP '1970-01-01 00:00:00'
        + CAST(round(pace_seconds) AS INTEGER) * INTERVAL 1 SECOND,
        '%M:%S'
    ) AS total_time_mm_ss,
    printf(
        '%d:%02d.%02d',
        CAST(floor(CAST(round(pace_seconds * 100) AS INTEGER) / 6000) AS INTEGER),            -- minutes
        CAST(floor((CAST(round(pace_seconds * 100) AS INTEGER) % 6000) / 100) AS INTEGER),    -- seconds
        CAST(CAST(round(pace_seconds * 100) AS INTEGER) % 100 AS INTEGER)                     -- hundredths
    ) AS pace_mm_ss_ff,
    log_id,
    filename,
    source_file
from (
    select
        "Number" as stroke_number,
        "Time (seconds)" as total_time,
        "Distance (meters)" as total_distance,
        case
            when try_cast("Pace (seconds)" as DOUBLE) is null then null
            when try_cast("Pace (seconds)" as DOUBLE) < 10 then try_cast("Pace (seconds)" as DOUBLE) * 60
            else try_cast("Pace (seconds)" as DOUBLE)
        end as pace_seconds,
        case
            when lower(machine_type) = 'bikeerg' then 1000
            else 500
        end as pace_split_m,
        "Watts" as watts,
        "Stroke Rate" as stroke_rate,
        season as season_year,
        "date" as workout_date,
        machine_type,
        try_cast(nullif(regexp_extract(filename, '([0-9]+)\\.csv$', 1), '') AS BIGINT) AS log_id,
        filename,
        regexp_extract(filename, '[^/]+$') as source_file
    from read_csv_auto('~/concept2/workouts/*detail*.csv', filename=true)
) s
where log_id is not null