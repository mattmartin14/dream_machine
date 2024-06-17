select
    hdr.order_id, hdr.crt_ts
    ,dtl.event_ts, dtl.event_desc
    ,ex.except_ts, ex.ex_desc
from wms.order_hdr as hdr
    inner join wms.order_event_dtl as dtl
        on hdr.order_id = dtl.order_id
    inner join (select distinct order_id from wms.order_event_dtl where event_type = 99) as dtlx
        on dtl.order_id = dtlx.order_id
    left join (
    
        select 
            dtl.order_id, dtl.event_ts
            ,ex.except_ts
            ,ex.ex_desc
        from wms.order_event_ex as ex
            inner join wms.order_event_dtl as dtl
                on ex.order_id = dtl.order_id
        where 1=1
        -- and dtl.order_id = 3
            and dtl.event_type = 99
            and ex.except_ts > dtl.event_ts
        qualify 1 = rank() 
            over(partition by dtl.order_id, dtl.event_ts order by datediff('second', dtl.event_ts, ex.except_ts))
    ) as ex
        on dtl.order_id = ex.order_id and dtl.event_ts = ex.event_ts
  order by hdr.order_id, dtl.event_ts