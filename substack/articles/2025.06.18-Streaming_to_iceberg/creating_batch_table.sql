create or replace table bufstream.email_updated_batch
using iceberg
as
select val.id
    , (kafka.event_timestamp - INTERVAL 1 hour) as event_timestamp
    , get_email() as old_email_address
    , val.old_email_address as new_email_address
from bufstream.email_updated