/* Optimization to use user agent for better sessionization */

CREATE TABLE marketplace_sessions (
  access_time timestamp,
  src_ip string,
  url string,
  agent string,
  session_start tinyint,
  session_id string
) STORED AS ORC;

INSERT INTO marketplace_sessions
SELECT *,
  concat(src_ip, concat('_', sum(session_start) OVER (PARTITION BY src_ip, agent ORDER BY access_time))) AS session_id
FROM (
  SELECT *,
    CASE
        WHEN unix_timestamp(access_time) - lag(unix_timestamp(access_time)) OVER (PARTITION BY src_ip, agent ORDER BY access_time) >= 1800 THEN 1 /* 30 mins idle */
        WHEN rank() OVER (PARTITION BY src_ip ORDER BY access_time) = 1 THEN 1 /* first hit ever */
        ELSE 0 /* within previous session */
    END AS session_start
  FROM marketplace
) sessions;


