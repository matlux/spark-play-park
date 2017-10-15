package basics

//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql._

object SparkIngestion {

  def main(args: Array[String]) {

    //val sc = new SparkContext(new SparkConf().setAppName("Scala Spark example").setMaster("local[*]"))

    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()

    //val spark = new SQLContext(sc)
    val opts = Map("header" -> "true",
                    "timestampFormat" -> "dd/MM/yyyy",
                    "inferSchema" -> "true")
    val df0 = spark.read.options(opts).csv("/home/mathieu/Dropbox/Finance/investment-transactions/Matlux_rate-setter_LenderTransactions_all_2017-07-31.csv")




    val df = df0.withColumn("month",month(column("date"))).withColumn("year",year(column("date")))
    //val df = spark.read.parquet("/home/mathieu/datashare/hdfs/parquet/test3")

    df.filter(column("year")==="2014").groupBy("year","month","type").sum("amount","capital","interest","fee").sort("year","month").show()
    df.filter(column("year")==="2014").groupBy("year","month","type").sum("amount").sort("year","month").show()
    df.filter(column("year")==="2014").groupBy("year","month").agg(sum("amount")).sort("year","month").show()
    df.filter(column("year")==="2014").groupBy("year","month").pivot("type").agg(sum("amount")).sort("year","month").show()
    df.groupBy("year","month").pivot("type").agg(sum("amount")).sort("year","month").show()
    df.groupBy("year","month").pivot("type",List("Bank transfer","Interest","RateSetter lender fee")).agg(sum("amount")).sort("year","month").show()


    //val wSpec2 = Window.partitionBy("year","month").orderBy("year","month").rowsBetween(Long.MinValue, 0)
    val wSpec2 = Window.orderBy("year","month").rowsBetween(Long.MinValue, 0)
    val pivotedReport = df.groupBy("year","month").pivot("type",List("Bank transfer","Interest","RateSetter lender fee")).agg(sum("amount")).sort("year","month")

    val finalReport = pivotedReport.withColumn("cum BT",sum(pivotedReport("Bank transfer")).over(wSpec2)).
      withColumn("cum interest",sum(pivotedReport("Interest")).over(wSpec2)).
      withColumn("cum fee",sum(pivotedReport("RateSetter lender fee")).over(wSpec2)).
      select("year","month","Bank transfer","cum BT","Interest","cum interest","RateSetter lender fee","cum fee")

    pivotedReport.withColumn("cum BT",sum(pivotedReport("Bank transfer")).over(wSpec2)).
      withColumn("cum interest",sum(pivotedReport("Interest")).over(wSpec2)).
      withColumn("cum fee",sum(pivotedReport("RateSetter lender fee")).over(wSpec2)).
      select("year","month","Bank transfer","cum BT","Interest","cum interest","RateSetter lender fee","cum fee").show()


    df.createOrReplaceTempView("transaction")

    df.groupBy(column("type"),year(column("date")),month(column("date"))).sum().show()
    df.groupBy(column("type"),window(column("date"), "31 days")).sum().sort("window").show()
    df.groupBy("year","month","type").sum().sort("year","month").show()
    df.groupBy().sum().show()
    df.agg(sum("interest")).show()

    df.select(month(column("date"))).show()


    spark.sql("select sum(interest) from transaction").show();

    df.write.save("/home/mathieu/datashare/hdfs/parquet/test")

    finalReport.coalesce(1).write .option("header", "true").csv("./hdfs/rateSetter_report4.cvs")

    df.write.partitionBy("year","month","type").mode(SaveMode.Append).save("./hdfs/parquet/test3")




    //df.repartition("entity", "year", "month", "day", "status").write.partitionBy("entity", "year", "month", "day", "status").mode(SaveMode.Append)

    //val wordsRDD = inputRDD.flatMap(x => x.split(" "))
    //val countRDD = wordsRDD.map(x => (x, 1))
     // .reduceByKey((x, y) => x + y)


    //println(inputRDD.schema)
    //println(inputRDD.collect().mkString("\n"))
    //println(inputRDD.count())

    System.out.println("========== Print Schema ============");
    df.printSchema();
    System.out.println("========== Print Data ==============");
    df.show();
    System.out.println("========== Print title ==============");
    df.select("Date").show();

  }

}
