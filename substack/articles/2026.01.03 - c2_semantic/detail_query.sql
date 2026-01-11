select "Number" as stroke_number
        ,"Time (seconds)" as total_time
        ,"Distance (meters)" as total_distance
        ,"Pace (seconds)" as pace
        ,"Watts" as watts
        ,"Stroke Rate" as stroke_rate
        ,season as season_year
        ,"date" as workout_date
        ,machine_type
        ,strftime(
            TIMESTAMP '1970-01-01 00:00:00'
            + CAST(round("pace") AS INTEGER) * INTERVAL 1 SECOND,
            '%M:%S'
        ) AS total_time_mm_ss
        ,printf(
            '%d:%02d.%02d',
            CAST(floor(CAST(round("pace" * 100) AS INTEGER) / 6000) AS INTEGER),            -- minutes
            CAST(floor((CAST(round("pace" * 100) AS INTEGER) % 6000) / 100) AS INTEGER),    -- seconds
            CAST(CAST(round("pace" * 100) AS INTEGER) % 100 AS INTEGER)                     -- hundredths
        ) AS pace_mm_ss_ff
        ,try_cast(nullif(regexp_extract(filename, '([0-9]+)\\.csv$', 1), '') AS BIGINT) AS log_id
        ,filename
        ,regexp_extract(filename, '[^/]+$') as source_file
    from read_csv_auto('~/concept2/workouts/*detail*.csv', filename=true)
    where nullif(regexp_extract(filename, '([0-9]+)\\.csv$', 1), '') is not null