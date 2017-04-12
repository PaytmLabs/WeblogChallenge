import java.net.URL

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Proof of concept that URL can be replaced by a number of hashes
 * to still be able to tell unique URLs
 */
object UrlByHash {
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: UrlByHash [input path] [output path]")
      return
    }

    val conf = new SparkConf().setAppName("Url by hash")
    val sc = new SparkContext(conf)

    val logData = sc.textFile(args(0))

    // 0. Load and parse
    val logPattern = "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d+)Z\\s\\S+\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):.+\\s\"(?:[A-Z]+)\\s(\\S+)\\sHTTP/".r
    val hits = logData
      .map(logPattern.findFirstMatchIn(_))
      .filter(!_.isEmpty) // filter out lines that haven't matched
      .map(line => {
        val url = new URL(line.get.subgroups(2)).getPath
        ( (url.hashCode, url.reverse.hashCode), Set(url))
      }) // ( hash1, hash2) -> url
      .reduceByKey( _ ++ _)
      .map( x => ( x._1, x._2.size))
      .filter( _._2 > 1)
      .saveAsTextFile( args(1))
  }
}
