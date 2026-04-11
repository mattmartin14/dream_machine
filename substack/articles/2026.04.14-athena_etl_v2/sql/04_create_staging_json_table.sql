CREATE EXTERNAL TABLE __STAGING_DATABASE__.__JSON_TABLE__ (
  order_id string,
  order_ts string,
  partition_date string,
  sales_channel string,
  fulfillment struct<method:string,warehouse:string,ship_speed:string>,
  customer_ref struct<customer_id:string,segment:string>,
  line_items array<struct<
    sku:string,
    name:string,
    category:string,
    quantity:int,
    pricing:struct<list_price:double,unit_price:double,currency:string>,
    discounts:array<struct<type:string,code:string,amount:double>>
  >>,
  totals struct<subtotal:double,shipping:double,tax:double,grand_total:double,currency:string>,
  payment struct<method:string,auth_code:string,status:string>,
  audit struct<ingested_at:string,source_system:string,trace_id:string>,
  customer_profile struct<
    name:struct<first:string,last:string>,
    contact:struct<email:string,sms_opt_in:boolean>,
    shipping_city:string
  >
)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json'='true')
LOCATION '__RAW_S3__'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.dt.type'='date',
  'projection.dt.range'='2020-01-01,NOW',
  'projection.dt.format'='yyyy-MM-dd',
  'storage.location.template'='__RAW_S3__dt=${dt}/orders/'
);
