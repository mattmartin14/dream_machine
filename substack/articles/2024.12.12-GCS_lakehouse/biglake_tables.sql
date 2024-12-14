CREATE OR REPLACE EXTERNAL TABLE bicycle_shop.ord_hdr
WITH CONNECTION us.test_cn_matt
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://data-mattm-test-sbx/bicycle_shop/processed/ord_hdr']
);


CREATE OR REPLACE EXTERNAL TABLE bicycle_shop.ord_dtl
WITH CONNECTION us.test_cn_matt
OPTIONS (
  format = 'DELTA_LAKE',
  uris = ['gs://data-mattm-test-sbx/bicycle_shop/processed/ord_dtl']
);


-- analytical query

SELECT hdr.order_id, sum(dtl.quantity) as tot_qty
FROM bicycle_shop.ord_hdr as hdr
  left join bicycle_shop.ord_dtl as dtl
    using (order_id)
group by all
order by 2 desc
limit 10