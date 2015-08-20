/* Build navigation around the site */

CREATE TABLE marketplace_navigation (
  url string,
  ref string,
  session_id string
) STORED AS ORC;

INSERT INTO marketplace_navigation
SELECT session_id, url, lag(url) OVER (PARTITION BY session_id ORDER BY access_time) as ref FROM (
  SELECT access_time, session_id, parse_url(url, 'PATH') as url FROM marketplace_sessions
) navigation;

/* Navigation graph */

CREATE TABLE marketplace_navigation_map (
  url string,
  ref string,
  clicks int
) STORED AS ORC;

INSERT INTO marketplace_navigation_map
SELECT url, ref, count(DISTINCT session_id) FROM marketplace_navigation
GROUP BY url, ref;

/* Unique entry pages */

/* Unique page navigation */
