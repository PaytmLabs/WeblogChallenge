CREATE EXTERNAL TABLE marketpalce_raw (
  access_time string,
  balancer string,
  src_ip string,
  dest_ip string,
  request_time float,
  backend_time float,
  response_time float,
  balancer_status tinyint,
  backend_status tinyint,
  request_size int,
  response_size int,
  request string,
  agent string,
  cipher string,
  tls string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = " ",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 'hdfs:///var/data/src';

