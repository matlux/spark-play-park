# spark-play-park
Me playing with Spark 2.2.0 feature in a Console to create reports with groupby, aggregations, pivots, etc... Also playing with Parquet and CVS persistance to test partitioning of data.


## Starting the Cloudera docker quick start image

    docker run -v /home/mathieu/datashare/dev:/home/mathieu/datashare/dev -p 8042:8042 -p 8088:8088 -p 8020:8020 -p 8888:8888 -p 11000:11000 --hostname=quickstart.cloudera --privileged=true -t -i cloudera/base2017-10-20 /usr/bin/docker-quickstart


## Create HDFS folders:

```bash
sudo -u hdfs hadoop fs -mkdir -p hdfs://localhost/user/clojspark/basics/inputdata
sudo -u hdfs hadoop fs -mkdir -p hdfs://localhost/user/dummy
sudo -u hdfs hadoop fs -chown -R dummy:dummy hdfs://localhost/user/clojspark/basics/inputdata
sudo -u hdfs hadoop fs -chown -R dummy:dummy hdfs://localhost/user/dummy
```

## Copy the input file into the HDFS folders

    hadoop fs -copyFromLocal input/wordcount-input.txt hdfs://localhost/user/clojspark/basics/inputdata

## Build Scala Assembly with SBT

    sbt assembly;

## Build jar + dependencies with maven

    mvn clean package

## Submit the job on YARN (build with SBT)

    spark-submit --class basics.SparkCountNoHDFS --master yarn target/scala-2.11/spark-play-park-assembly-0.1.jar dev

## Submit the job on Spark Standalone (built with maven)

    spark-submit --jars target/lib/config-1.3.1.jar --class basics.SparkCountNoHDFS --master spark://mathieu-XPS-13-9360:7077 target/spark-splay-park_2.11-1.0.0-SNAPSHOT.jar dev

## Submit the job on Spark Standalone (built with maven) and debug

    spark-submit --jars target/lib/config-1.3.1.jar --driver-java-options -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 --conf spark.driver.host=<local ip> --conf spark.executor.cores=4  --class basics.SparkCountNoHDFS --master spark://mathieu-XPS-13-9360:7077 target/spark-splay-park_2.11-1.0.0-SNAPSHOT.jar dev
