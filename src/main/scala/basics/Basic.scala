package basics

import config.MyConfig
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


class Person(val firstName: String, val lastName: String) {

  println("the constructor begins")
  val fullName = firstName + " " + lastName

  val HOME = System.getProperty("user.home");

  // define some methods
  def foo { println("foo") }
  def printFullName {
    // access the fullName field, which is created above
    println(fullName)
  }

  printFullName
  println("still in the constructor")

}

object SparkWordCount {



  def main(args: Array[String]) {

    val envName = if (args.size == 1) {
      val envName = args(0)
      Some(envName)
    } else {
      None
    }

    val config = new MyConfig(envName)

    val inputData = config.getString("basic.inputData")
    val outputData = config.getString("basic.outputData")
    val master = config.getString("basic.master")


    println(s"inputData is $inputData")
    println(s"outputData is $outputData")
    println(s"master is $master")

    val sc = if (master=="yarn") new SparkContext(new SparkConf().setAppName("Scala Spark example"))
    else new SparkContext(new SparkConf().setAppName("Scala Spark example").setMaster(master))

    val inputRDD =
      sc.textFile(inputData + "/wordcount-input.txt")

    val wordsRDD = inputRDD.flatMap(x => x.split(" "))
    val countRDD = wordsRDD.map(x => (x, 1))
      .reduceByKey((x, y) => x + y)


    println(countRDD.collect().mkString(", "))

  }

}
