package basics


import java.io.File

import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ConcatenateFC {

  val opts = Map("header" -> "true",
    "dateFormat" -> "yyyy-MM-dd",
    "inferSchema" -> "true")

  val inputData = "/home/mathieu/Dropbox/Finance/investment-transactions/fundingCircleMonthly"
  val outputData = "/home/mathieu/Dropbox/Finance/investment-transactions/"

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  def concatenate(spark : SparkSession): Unit = {

    val decimalType : DecimalType = DataTypes.createDecimalType(15, 2)

    val customSchema = StructType(Array(
      StructField("Date", DateType, true),
      StructField("Description", StringType, true),
      StructField("Paid In", decimalType, true),
      StructField("Paid Out", decimalType, true)))

    val listFiles = getListOfFiles(new File(inputData),List("csv")).filter(f =>  f.getName.matches("Matlux_funding-circles_.*")).sorted

    val dateRange = listFiles.flatMap(f =>
      "Matlux_funding-circles_(.*)_\\d{4}-\\d{2}-.._..-..-..\\.csv".r.findFirstMatchIn(f.getName) match {
        case Some(i) => List(i.group(1))
        case None => List()
      }).sorted

    dateRange.head
    dateRange.last


    val dfs = listFiles.map(f =>
      spark.read.format("csv")
        .options(opts)
        .schema(customSchema)
        .csv(f.getCanonicalPath))
    val df = dfs.reduceLeft((acc,df) => acc.union(df)).sort("Date")
    //val dfs = listFiles.map(f => spark.read.options(opts).csv(f.getCanonicalPath))
    //val df = spark.read.options(opts).csv(inputData + "/Matlux_rate-setter_LenderTransactions_all_2017-07-31.csv")

    val df2 = df.filter(!column("Description").rlike("Loan offer")).
        withColumn("Principal",when(column("Description").rlike(".*Principal (.+), Interest.*"),regexp_replace(column("Description"),".*Principal (.+), Interest.*","$1")) )

    //df2.count()
    df2.sort(asc("Date"),desc("Paid In")).show(50,false)

    val df3 = df.filter(!column("Description").rlike("Loan offer")).
      withColumn("Principal",regexp_replace(column("Description"),".*Principal (.+), Interest.*","$1"))


    //val df = dfs(0)
    //df.coalesce(1).write .option("header", "true").csv(outputData + f"/Matlux_funding-circles_test_${dateRange.head}_${dateRange.last}.cvs")


    System.out.println("========== Print Schema ============")
    df.printSchema()
    System.out.println("========== Print Data ==============")
    df.show(50,false)
    System.out.println("========== Print title ==============")
    df.select("Date").show()

  }

  def main(args: Array[String]) {



    val master = "local[*]"

    //val sc = new SparkContext(new SparkConf().setAppName("Scala Spark example").setMaster("local[*]"))

    val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()



    concatenate(spark )

  }

}
