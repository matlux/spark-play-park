package basics

import config.MyConfig
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext, TaskContext}


object SparkSimpleCount {



  def main(args: Array[String]) {

    val log = Logger.getRootLogger

    val envName = if (args.size == 1) {
      val envName = args(0)
      Some(envName)
    } else {
      None
    }

    val config = new MyConfig(envName)

    val master = config.getString("basic.master")

    println(s"master is $master")

    val appName = "SparkSimpleCount"
    
    val sc = if (master=="yarn") new SparkContext(new SparkConf().setAppName(appName))
    else new SparkContext(new SparkConf().setAppName(appName).setMaster(master))


    val count = sc.parallelize(1 to 100).count


    println(count)

    println("press enter to continue...")
    scala.io.StdIn.readLine()

  }

}
