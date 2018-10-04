package basics

import config.MyConfig
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext, TaskContext}


object SparkCountNoHDFSWithInstrument {

  // very important for serialization to keep this at object level
  val log = Logger.getRootLogger

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

    val appName = "SparkCountNoHDFSWithInstrument"

    val sc = if (master=="yarn") new SparkContext(new SparkConf().setAppName(appName))
    else new SparkContext(new SparkConf().setAppName(appName).setMaster(master))


    val inputRDD = sc.parallelize(List("The quick brown fox jumps over the lazy dog",
      "The earliest known appearance of the phrase is",
      "A favorite copy set by writing teachers for their pupils",
      "is the following, because it contains every letter of the alphabet",
      "This is just a fifth line."),10)
//      sc.textFile(inputData + "/wordcount-input.txt")

    log.info("inputRDD partition size = " + inputRDD.partitions.size)

    val wordsRDD = inputRDD.flatMap{x =>
      val threadId = Thread.currentThread.getId
      val tc = TaskContext.get
      log.info("taskid=" + tc.taskAttemptId + s", start, ************* Threadid=$threadId, split($x) *****")
      Thread.sleep(7000)
      log.info("taskid=" + tc.taskAttemptId + s", stop split($x) *********")
      x.split(" ")}
    log.info("wordsRDD partition size = " + wordsRDD.partitions.size)
    val pairedRDD = wordsRDD.map{x =>
      val threadId = Thread.currentThread.getId
      val tc = TaskContext.get
      log.info("taskid=" + tc.taskAttemptId + s", start, ************* Threadid=$threadId, pairedRDD($x)")
      Thread.sleep(1000)
      log.info("taskid=" + tc.taskAttemptId + s", stop pairedRDD($x) ****")
      (x, 1)}
    log.info("pairedRDD partition size = " + pairedRDD.partitions.size)
    val countRDD = pairedRDD.reduceByKey{(x, y) =>
      val threadId = Thread.currentThread.getId
      val tc = TaskContext.get
      log.info("taskid=" + tc.taskAttemptId + s", start, ************* Threadid=$threadId,  reduceByKey($x, $y)")
      Thread.sleep(13000)
      log.info("taskid=" + tc.taskAttemptId + s", stop reduceByKey($x, $y) **")
      x + y}
    log.info("countRDD partition size = " + countRDD.partitions.size)


    println(countRDD.collect().mkString(", "))

    println("press enter to continue...")
    scala.io.StdIn.readLine()

  }

}
