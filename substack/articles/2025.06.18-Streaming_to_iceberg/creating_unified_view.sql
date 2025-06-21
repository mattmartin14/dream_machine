CREATE OR REPLACE VIEW bufstream.v_email_update_nrt
as
SELECT src_sys_ind, id, event_timestamp, old_email_address, new_email_address
FROM (
SELECT *
    ,RANK() OVER(PARTITION BY ID ORDER BY event_timestamp desc) as rnk
FROM (
    select "STREAM" as src_sys_ind
        ,val.id, kafka.event_timestamp, val.old_email_address, val.new_email_address
    from bufstream.email_updated
    UNION ALL 
    select "BATCH" AS src_sys_ind
        ,id, event_timestamp, old_email_address, new_email_address
    from bufstream.email_updated_batch
) as sub
) AS s2
WHERE rnk = 1
--QUALIFY RANK() OVER(PARTITION BY ID ORDER BY event_timestamp desc) = 1 