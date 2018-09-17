package basics

import config.MyConfig
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf




object SparkCountNoHDFS {



  def main(args: Array[String]) {

    val envName = if (args.size == 1) {
      val envName = args(0)
      Some(envName)
    } else {
      None
    }

    val config = new MyConfig(envName)

    val master = config.getString("basic.master")

    println(s"master is $master")

    val appName = "SparkCountNoHDFS"
    
    val sc = if (master=="yarn") new SparkContext(new SparkConf().setAppName(appName))
    else new SparkContext(new SparkConf().setAppName(appName).setMaster(master))


    val inputRDD = sc.parallelize(List("The quick brown fox jumps over the lazy dog",
      "The earliest known appearance of the phrase is",
      "A favorite copy set by writing teachers for their pupils",
      "is the following, because it contains every letter of the alphabet",
      "This is just a fifth line."))
//      sc.textFile(inputData + "/wordcount-input.txt")

    val wordsRDD = inputRDD.flatMap(x => x.split(" "))
    val pairedRDD = wordsRDD.map(x => (x, 1))
    val countRDD = pairedRDD.reduceByKey((x, y) => x + y)


    println(countRDD.collect().mkString(", "))

    println("press enter to continue...")
    scala.io.StdIn.readLine()

  }

}
