#!/bin/bash

# Moves data from local file system to HDFS

hadoop fs -mkdir -p /var/data/src
hadoop fs -put /tmp/2015_07_22_mktplace_shop_web_log_sample.log.gz /var/data/src/

