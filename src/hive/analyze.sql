/* Average session time in seconds */
SELECT avg(session_time) FROM
(SELECT session_id, unix_timestamp(max(access_time)) - unix_timestamp(min(access_time)) as session_time
  FROM marketplace_sessions
  GROUP BY session_id
) sessions;

/* Unique URLs per session */
SELECT session_id, count(distinct url) as url_count FROM marketplace_sessions GROUP BY session_id order by url_count desc;

/* Top 10 most engaged users */
SELECT src_ip, session_id, unix_timestamp(max(access_time)) - unix_timestamp(min(access_time)) as session_time
FROM marketplace_sessions
GROUP BY src_ip, session_id
ORDER BY session_time DESC
LIMIT 10;




