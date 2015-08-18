CREATE EXTERNAL TABLE marketpalce_raw (
  access_time string,
  realm string,
  src_ip string,
  dest_ip string,
  metric_1 float,
  metric_2 float,
  metric_3 float,
  status_1 tinyint,
  status_2 tinyint,
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

