sbt clean assembly

startTime=`date +%s`

$SPARK_HOME/bin/spark-submit target/scala-2.11/PaytmWeblogChallenge.jar $1

endTime=`date +%s`
runningTime=$((endTime-startTime))

echo "execution duration: ${runningTime} seconds "
