import java.net.URL
import java.util.UUID

import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer

/**
 * Optimized version that sessionizes as much as possible on workers
 */
object WebLogessionizeOptimized {
  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Usage: WebLogessionizeOptimized [timeout in seconds] [input path] [output path]")
      return
    }

    val conf = new SparkConf().setAppName("Web Log Optimized")
    val sc = new SparkContext(conf)

    val timeout = args(0).toInt * 1000
    val logData = sc.textFile(args(1))

    // 0. Load and parse
    val logPattern = "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d+)Z\\s\\S+\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):.+\\s\"(?:[A-Z]+)\\s(\\S+)\\sHTTP/".r
    val sessions = logData
      .map(logPattern.findFirstMatchIn(_))
      .filter(!_.isEmpty) // filter out lines that haven't matched
      .map(line => {
          val url = new URL(line.get.subgroups(2)).getPath
          val time = new DateTime( line.get.subgroups(0)).toDate.getTime
          (
              line.get.subgroups(1),
              List( (time, time, Set( (url.hashCode, url.reverse.hashCode))) )
          )
      }) // ip -> [( startTime, endTime, [(url.hash1, url.hash2)] )]
      .reduceByKey( ( session1, session2) => {
        val sessions = ( session1 ++ session2).sortBy( _._1)
        var merged = new ListBuffer[( Long, Long, Set[(Int, Int)])]

        var prevSession = sessions(0)
        for( nextSession <- sessions.drop(1)) {
          if( nextSession._1 - prevSession._2 > timeout) {
            // expired, start a new one
            merged += prevSession
            prevSession = nextSession
          } else {
            // merge two sessions
            prevSession = ( prevSession._1, Math.max( prevSession._2, nextSession._2), prevSession._3 ++ nextSession._3)
          }
        }
        merged += prevSession

        merged.toList
    }) // ip -> [( startTime, endTime, [(url.hash1, url.hash2)] )]
    .flatMap( byIp => byIp._2.map( session => (byIp._1, session._1, session._2, session._3))) // ip, startTime, endTime, [(hash1,hash2)]
    .map( session => ( session._1, UUID.randomUUID().toString, session._3-session._2, session._4.size)) // start/end -> duration and number of unique urls, assign sessionId
    .filter(_._2 > 0) // filter single hit sessions
    .cache() // ip, duration, [(hash1,hash2)]

    // 2. Determine the average session time

    val avgTime = sessions.map( _._3).mean / 1000 // in seconds

    // 3. Determine unique URL visits per session

    val mostUrls = sessions
      .sortBy( session => session._4, false)
      .take(10)

    // 4. Find the most engaged users
    val mostEngaged = sessions.sortBy( _._3, false).take(10)

    sessions.saveAsTextFile(args(2))

  }
}

