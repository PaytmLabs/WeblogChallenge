REGISTER '/usr/lib/pig/piggybank.jar';
REGISTER '/usr/lib/pig/weblog-sessionize-0.1.jar';

DEFINE MarkSessions web.log.challenge.pig.MarkSessions('900'); -- 15 minutes

log = LOAD '/var/data/src/' USING org.apache.pig.piggybank.storage.CSVExcelStorage(' ') 
     as (
          timestamp:chararray,
          balancer:chararray,
          src:chararray,
          dest:chararray,
          request_time:float,
          backend_time:float,
          response_time:float,
          balancer_status:int,
          backend_status:int,
          request_size:int,
          response_size:int,
          request:chararray,
          agent:chararray,
          cipher:chararray,
          tls:chararray
     );

-- clean data
transform = FOREACH log GENERATE
  ToDate( timestamp) as access_time,
  REGEX_EXTRACT( src, '([^:]+)', 1) as src_ip,
  REGEX_EXTRACT( request, '[^\\s]+\\s([^\\s]+)\\s', 1) as url,
  agent;

-- sessionize
sessions = FOREACH (GROUP transform BY src_ip) {
     ordered = ORDER transform BY access_time;
     GENERATE FLATTEN(MarkSessions(ordered)) AS (access_time, src_ip, url, agent, session_id, session_duration);
};

STORE sessions INTO 'weblog-sessionized';
