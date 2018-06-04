package mehrdad.paytmlabs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.types.{TimestampType, StringType, DoubleType, LongType}

object Main {

  // columns based on: http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format

  val TIMESTAMP_COL = "timestamp"
  val ELB_COL = "elb"
  val CLIENT_IP_COL = "client_ip"
  val BACKEND_IP_COL = "backend_ip"
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

  val schema = StructType(Array(
    StructField(TIMESTAMP_COL, TimestampType, true),
    StructField(ELB_COL, StringType, true),
    StructField(CLIENT_IP_COL, StringType, true),
    StructField(BACKEND_IP_COL, StringType, true),
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
  )

  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Weblog Challenge").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = args(0)
    val rawDf = readLogs(spark, inputPath)

    rawDf.show(5, false)
  }

  def readLogs(spark: SparkSession, inputPath: String): DataFrame = {
    spark.read
      .option("header", true)
      .option("delimiter", " ")
      .option("header", false)
      .schema(schema)
      .csv(inputPath)
  }

}
