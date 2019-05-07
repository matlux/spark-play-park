package basics

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, FunSuite, Matchers}

class ParquetBitemporalTest  extends FlatSpec with Matchers {
  "42" should "equal to 42" in {


    val master = "local[*]"

    //val sc = new SparkContext(new SparkConf().setAppName("Scala Spark example").setMaster("local[*]"))

    val sBuilder = SparkSession.builder.appName("Simple Application")

    val spark : SparkSession = (if (master=="yarn") {
      sBuilder
    } else {sBuilder.master(master)}).getOrCreate()

    ConcatenateFC.concatenate(spark)

    assert(42 === 42)
  }


}
