#!/bin/bash

# Create HDFS folders:
hadoop fs -mkdir -p hdfs://localhost/user/dummy/inputdata
hadoop fs -mkdir -p hdfs://localhost/user/dummy/outputdata
#hadoop fs -chown -R dummy:dummy hdfs://localhost/user/dummy/inputdata

# Copy the input file into the HDFS folders
hadoop fs -copyFromLocal input/wordcount-input.txt hdfs://localhost/user/dummy/inputdata
hadoop fs -copyFromLocal /home/mathieu/datashare/dev/Matlux_rate-setter_LenderTransactions_all_2017-07-31.csv hdfs://localhost/user/dummy/inputdata

# Build Scala Package
sbt package;

# Submit the job on YARN
spark-submit --class basics.SparkWordCount --master yarn target/scala-2.11/spark-play-park_2.11-0.1-SNAPSHOT.jar
spark-submit --class basics.SparkIngestion --master yarn target/scala-2.11/spark-play-park_2.11-0.1-SNAPSHOT.jar

