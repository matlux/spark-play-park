package basics

import config.MyConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object SparkCountNoHDFSNoSparkSubmit {



  def main(args: Array[String]) {

    //val log = Logger.getRootLogger

    val envName = if (args.size == 1) {
      val envName = args(0)
      Some(envName)
    } else {
      None
    }

    val config = new MyConfig(envName)

    val master = config.getString("spark.master")

    println(s"master is $master")

    val appName = "SparkCountNoHDFS"

    val sc = SparkSession.builder.appName(appName).getOrCreate()

    sparkClientRunCountNoHDFS(sc)


    println("press enter to continue...")
    scala.io.StdIn.readLine()

  }

  def sparkClientRunCountNoHDFS(sc: SparkSession ) {
    val inputRDD = sc.sparkContext.parallelize(List("The quick brown fox jumps over the lazy dog",
      "The earliest known appearance of the phrase is",
      "A favorite copy set by writing teachers for their pupils",
      "is the following, because it contains every letter of the alphabet",
      "This is just a fifth line."))
    //      sc.textFile(inputData + "/wordcount-input.txt")

    val wordsRDD = inputRDD.flatMap(x => x.split(" "))
    val pairedRDD = wordsRDD.map(x => (x, 1))
    val countRDD = pairedRDD.reduceByKey((x, y) => x + y)


    println(countRDD.collect().mkString(", "))
  }

}
