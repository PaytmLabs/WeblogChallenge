CREATE TABLE marketplace (
  access_time timestamp,
  src_ip string,
  url string,
  agent string
) STORED AS ORC;

INSERT INTO TABLE marketplace
SELECT
  cast( concat(substr(access_time,1,10),' ',substr(access_time,12,12)) as timestamp) as access_time,
  regexp_extract( src_ip, '([^:]+)') as src_ip,
  regexp_extract( request, '[^\\s]+\\s([^\\s]+)\\s') as url,
  agent
FROM marketpalce_raw;

/* check if agent adds much to user identification */
select src_ip, count(distinct agent) from marketplace group by src_ip having count(distinct agent) > 1;


