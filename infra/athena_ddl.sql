CREATE DATABASE IF NOT EXISTS rees46;
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS intermediate;
CREATE DATABASE IF NOT EXISTS core;
CREATE DATABASE IF NOT EXISTS marts;

CREATE EXTERNAL TABLE IF NOT EXISTS rees46.events_silver (
  event_id string,
  event_time timestamp,
  event_type string,
  product_id bigint,
  category_id bigint,
  category_code string,
  brand string,
  price double,
  user_id bigint,
  user_session string
)
PARTITIONED BY (
  event_date date
)
STORED AS PARQUET
LOCATION 's3://BUCKET_NAME/silver/events/'
TBLPROPERTIES (
  'classification' = 'parquet',
  'projection.enabled' = 'true',
  'projection.event_date.type' = 'date',
  'projection.event_date.format' = 'yyyy-MM-dd',
  'projection.event_date.range' = '2019-10-01,2019-11-30',
  'storage.location.template' = 's3://BUCKET_NAME/silver/events/event_date=${event_date}/'
);
