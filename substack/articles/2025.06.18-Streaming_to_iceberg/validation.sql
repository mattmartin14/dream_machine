select "STREAM" as src_sys_ind
    ,val.id, kafka.event_timestamp, val.old_email_address, val.new_email_address
from bufstream.email_updated
where val.id = '9c3d883a-1c99-4ee0-bb39-becd7e581042'
UNION ALL 
select "BATCH" AS src_sys_ind
    ,id, event_timestamp, old_email_address, new_email_address
from bufstream.email_updated_batch
where ID = '9c3d883a-1c99-4ee0-bb39-becd7e581042'
UNION ALL
SELECT *
from bufstream.v_email_update_nrt
where ID = '9c3d883a-1c99-4ee0-bb39-becd7e581042'
ORDER BY 1