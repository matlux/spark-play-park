package basics

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

    val sc = new SparkContext(new SparkConf().setAppName("Scala Spark example").setMaster("local[*]"))

    val inputRDD =
      sc.textFile("file:/home/mathieu/dev/clojspark-mat/input/wordcount-input.txt")

    val wordsRDD = inputRDD.flatMap(x => x.split(" "))
    val countRDD = wordsRDD.map(x => (x, 1))
      .reduceByKey((x, y) => x + y)


    println(countRDD.collect().mkString(", "))

  }

}
