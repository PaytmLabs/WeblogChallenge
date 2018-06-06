package mehrdadap.paytmlabs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{TimestampType, StringType, DoubleType, LongType}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Main {

  val INACTIVE_TIME_THRESHOLD_MIN = 15 // in minutes
  // original columns, based on: http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format

  val TIMESTAMP_COL = "timestamp"
  val ELB_COL = "elb"
  val CLIENT_IP_PORT_COL = "client_ip_port"
  val BACKEND_IP_PORT_COL = "backend_ip_port"
  val REQUEST_TIME_COL = "request_time"
  val BACKEND_TIME_COL = "backend_time"
  val RESPONSE_TIME_COL = "response_time"
  val ELB_STATUS_CODE_COL = "elb_status_code"
  val BACKEND_STATUS_CODE_COL = "backend_status_code"
  val RECEIVED_BYTES_COL = "received_bytes"
  val SENT_BYTES_COL = "sent_bytes"
  val REQUEST_COL = "request"
  val USER_AGENT_COL = "user_agent"
  val SSL_CIPHER_COL = "ssl_cipher"
  val SSL_PROTOCOL_COL = "ssl_protocol"


  // added derived columns
  val SESSION_ID_COL = "session_id"
  val SESSION_DURATION = "session_duration_in_seconds"
  val TOTAL_SESSION_DURATION = "total_session_duration_in_seconds"
  val URL_PER_SESSION_COUNT_COL = "url_per_session_count"
  val CLIENT_IP_COL = "client_ip"

  val schema = StructType(Array(
    StructField(TIMESTAMP_COL, TimestampType, true),
    StructField(ELB_COL, StringType, true),
    StructField(CLIENT_IP_PORT_COL, StringType, true),
    StructField(BACKEND_IP_PORT_COL, StringType, true),
    StructField(REQUEST_TIME_COL, DoubleType, true),
    StructField(BACKEND_TIME_COL, DoubleType, true),
    StructField(RESPONSE_TIME_COL, DoubleType, true),
    StructField(ELB_STATUS_CODE_COL, StringType, true),
    StructField(BACKEND_STATUS_CODE_COL, StringType, true),
    StructField(RECEIVED_BYTES_COL, LongType, true),
    StructField(SENT_BYTES_COL, LongType, true),
    StructField(REQUEST_COL, StringType, true),
    StructField(USER_AGENT_COL, StringType, true),
    StructField(SSL_CIPHER_COL, StringType, true),
    StructField(SSL_PROTOCOL_COL, StringType, true)
  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("PaytmLabs Weblog Challenge").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = args(0)
    val rawDf = readLogs(spark, inputPath)

    // a quick view of the data
    rawDf.printSchema()

    val sessionizedDf = sessionize(rawDf)
    sessionizedDf.cache()

    sessionizedDf.show(10)

    // Finding average duration of all sessions
    println("-"*10 + " average duration all sessions  " + "-"*10)
    sessionizedDf.agg(avg(col(SESSION_DURATION))).show(false)

    // There could possibly be more than one ip with max duration time
    val mostEngagedIpsDf: DataFrame = findMostEngagedIPs(sessionizedDf)
    println("-"*10 + " most engaged IPs " + "-"*10)
    println(s"number of most engaged IPs: ${mostEngagedIpsDf.count}")
    mostEngagedIpsDf.show()

    // unique urls visited per session
    val urlPerSessionDf = sessionizedDf.select(CLIENT_IP_PORT_COL, USER_AGENT_COL, SESSION_ID_COL, URL_PER_SESSION_COUNT_COL)
    println("-"*10 + " URLs visited per session " + "-"*10)
    urlPerSessionDf.show()

    spark.close()
  }

  def readLogs(spark: SparkSession, inputPath: String): DataFrame = {
    spark.read
      .option("delimiter", " ")
      .option("header", false)
      .schema(schema)
      .csv(inputPath)
  }

  def sessionize(df: DataFrame): DataFrame = {
    /**
      * Assumptions:
      *         -Session where session_time = 0 will be filtered out
      *         -
      * output:
      *   Dataframe with the following columns:
      *     CLIENT_IP_PORT_COL, USER_AGENT_COL, SESSION_ID_COL, SESSION_DURATION, URL_PER_SESSION_COUNT_COL
      */


    val IS_NEW_SESSION_COL = "is_new_session"
    val TIMESTAMP_LONG_COL = "timestamp_long"

    val timeOrderedWin = Window.partitionBy(CLIENT_IP_PORT_COL, USER_AGENT_COL).orderBy(TIMESTAMP_LONG_COL)

    val dfWithSessionId = df
      .withColumn(TIMESTAMP_LONG_COL, col(TIMESTAMP_COL).cast("long"))
      .withColumn(IS_NEW_SESSION_COL,
        (col(TIMESTAMP_LONG_COL) - (lag(col(TIMESTAMP_LONG_COL), offset = 1, defaultValue = 0).over(timeOrderedWin)) > (INACTIVE_TIME_THRESHOLD_MIN * 60)).cast("long"))
      .withColumn(SESSION_ID_COL, sum(IS_NEW_SESSION_COL).over(timeOrderedWin))
      .drop(IS_NEW_SESSION_COL)

    val sessionizedDf = dfWithSessionId.groupBy(CLIENT_IP_PORT_COL, USER_AGENT_COL, SESSION_ID_COL)
      .agg((max(TIMESTAMP_LONG_COL) - min(TIMESTAMP_LONG_COL)).alias(SESSION_DURATION),
        countDistinct(REQUEST_COL).alias(URL_PER_SESSION_COUNT_COL))
      .filter(col(SESSION_DURATION) =!= 0)

    sessionizedDf
  }

  def findMostEngagedIPs(sessionizedDf: DataFrame): DataFrame = {
    /**
      * Extracting client IP address from ip:port and sum duration of all sessions for each ip
      *
      * output:
      *   Dataframe with columns: CLIENT_IP_COL, TOTAL_SESSION_DURATION
      */

    val sessionsTimeByIpDf = sessionizedDf.withColumn(CLIENT_IP_COL, split(col(CLIENT_IP_PORT_COL), ":")(0))
      .groupBy(CLIENT_IP_COL)
      .agg(sum(SESSION_DURATION).alias(TOTAL_SESSION_DURATION)).cache()

    val longestSessionTime: Long = sessionsTimeByIpDf.select(max(col(TOTAL_SESSION_DURATION))).take(1).head.getAs[Long](0)
    val mostEngagedIpsDf = sessionsTimeByIpDf.filter(col(TOTAL_SESSION_DURATION) === longestSessionTime)
      .select(CLIENT_IP_COL, TOTAL_SESSION_DURATION)


    mostEngagedIpsDf
  }

}
