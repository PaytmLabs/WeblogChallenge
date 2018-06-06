#!/bin/bash

# creating uber jar
sbt clean assembly

# to calculate running time
startTime=`date +%s`

input=$1
if [ ${input: -3} == ".gz" ]; then
    echo "uncompressing $input file..."
    gunzip $input

    input=${input%.gz*} # removing .gz from the file name
fi

$SPARK_HOME/bin/spark-submit target/scala-2.11/PaytmWeblogChallenge.jar $input

endTime=`date +%s`
runningTime=$((endTime-startTime))

echo "execution duration: ${runningTime} seconds"
