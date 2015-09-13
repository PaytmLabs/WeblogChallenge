import java.util.UUID

import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.{Period, Seconds, DateTime}
import scala.collection.mutable.{Buffer, ListBuffer}

/**
 * Sessionize Web Log
 */
object WebLogSessionize {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Web Log").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val logData = sc.textFile("../data/2015_07_22_mktplace_shop_web_log_sample.log.gz")

    // 0. Load and parse
    val logPattern = "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d+)Z\\s\\S+\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):.+\\s\"(?:POST|GET)\\s(\\S+)\\sHTTP/".r
    val hits = logData
        .map( logPattern.findFirstMatchIn(_))
        .filter( !_.isEmpty) // filter out lines that haven't matched
        .map( line => ( line.get.subgroups(1), List( ((line.get.subgroups(0), line.get.subgroups(2) )) ))) // ip -> time, url

    // 1. Sessionize the web log by IP

    val sessions = hits.reduceByKey( _ ++ _).flatMap( sessionize) // ( ip, sessionId, [ (time, duration, url) ])

    // 2. Determine the average session time

    val sessionTimes = sessions
      .map( session => { ((session._1 , session._3.last._2)) } ) // (ip, last session hit)
      .filter( _._2 > 0) // filter out single-hit sessions
      .cache()

    val avgTime = sessionTimes.map( _._2).mean()

    // 3. Determine unique URL visits per session

    val sessionUrls = sessions.map( session => { ( session._1, session._2, session._3.map( _._3).distinct.size)})
    val mostUrls = sessionUrls.sortBy( _._3, false).take(10)

    // 4. Find the most engaged users
    val mostEngaged = sessionTimes.sortBy( _._2, false).take(10)

    sessions.saveAsTextFile("../data/output")
  }

  val expirePeriod = Period.minutes( 30)

  def sessionize( ip: (String, List[(String, String)]) ): Buffer[(String, String, Buffer[(DateTime, Int, String)])] = {
    var startTime:DateTime = null
    var expireTime:DateTime = null

    val sessions = new ListBuffer[(String, String, Buffer[(DateTime, Int, String)])]
    var session:ListBuffer[(DateTime, Int, String)] = null

    for( hit <- ip._2.sortBy( _._1)) {
      val accessTime = new DateTime(hit._1)
      if(expireTime == null || accessTime.isAfter(expireTime)) { // first hit ever or a new session
        if( session != null) {
          sessions += ((ip._1, UUID.randomUUID().toString, session))
        }

        startTime = accessTime
        session = new ListBuffer[(DateTime, Int, String)]
      }
      expireTime = accessTime.plus( expirePeriod)
      session += ((accessTime, Seconds.secondsBetween(startTime, accessTime).getSeconds(), hit._2))
    }

    if( session != null) {
      sessions += ((ip._1, UUID.randomUUID().toString, session))
    }

    sessions
  }

}
