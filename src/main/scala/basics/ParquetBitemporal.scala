package basics

//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
import basics.ConcatenateFC.concatenate
import config.MyConfig
import org.apache.spark
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql._
import com.typesafe.config.ConfigFactory

object ParquetBitemporal {

  val inputData = ConcatenateFC.inputData
  val outputData = "./outputdata"

  def process(spark : SparkSession) {


    //val spark = new SQLContext(sc)
    val opts = Map("header" -> "true",
                    "timestampFormat" -> "dd/MM/yyyy",
                    "inferSchema" -> "true")
    val df0 = spark.read.options(opts).csv(inputData + "/../Matlux_rate-setter_LenderTransactions_all_2014-03-24_2018-11-29.csv")



//dfrs0
    val df = df0
    //val df = df0.withColumn("month",month(column("date"))).withColumn("year",year(column("date"))).withColumn("day",dayofmonth(column("date")))
    //val df = spark.read.parquet("/home/mathieu/datashare/hdfs/parquet/test3")

    //val df0 = dfrs0.filter(column("date").gt(lit("2018-04-05"))).show

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

    finalReport.show(50,false)
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

    df.select(column("year"),column("month"),column("day")).distinct().sort("year","month","day").show()
    df.select(column("year"),column("month"),column("day"),column("amount")).distinct().sort("year","month","day").show()


//    val timeSeries1 = df.groupBy("year","month","day").agg(last("amount")).sort("year","month","day")
//    val timeSeries2 = df.groupBy("year","month","day").agg(first("amount")).sort("year","month","day")
    val timeSeries1 = df.filter(column("date").lt(lit("2018-11-27"))).
              groupBy("date").
              agg(last("amount")).
              withColumnRenamed("last(amount, false)","value").
              withColumn("value2",when(col("date").equalTo(lit("2014-04-01")),lit(2305)).otherwise(col("value"))).
              drop("value").
              withColumnRenamed("value2","value").
              sort("date")
    val timeSeries2 = df.filter(column("date").lt(lit("2018-11-28"))).groupBy("date").agg(last("amount")).withColumnRenamed("last(amount, false)","value").sort("date")
    val timeSeries3 = df.groupBy("date").agg(last("amount")).withColumnRenamed("last(amount, false)","value").sort("date")
    timeSeries1.show()
    timeSeries1.printSchema()
    timeSeries2.show()
    timeSeries1.first()
    timeSeries3.show()
    timeSeries1.select(first("date")).show()
    timeSeries1.select(last("date")).show()
    timeSeries2.select(last("date")).show()
    timeSeries3.select(last("date")).show()
//      .withColumn("extract_time",to_date(lit("2018-11-26")))
//      .withColumn("extract_time",to_date(lit("2018-11-27")))
//      .withColumn("extract_time",to_date(lit("2018-11-28")))

    spark.sql("select sum(interest) from transaction").show();

    df.write.save(outputData + "/parquet/test")

    finalReport.coalesce(1).write .option("header", "true").csv(outputData + "/rateSetter_report4.cvs")

    //.show()
    writeParquet(addExtrationTime(timeSeries1,"2018-11-26"),outputData + "/parquet/timeSeries1")

    val t1 = readParquet(spark, outputData + "/parquet/timeSeries1")
    t1.show()
    val newT1 = getLatestData(t1)

    timeSeries1.show()
    newT1.show()

    val timeSeriesToAppend = timeSeries2.except(newT1)
    timeSeriesToAppend.count()

    writeParquet(addExtrationTime(timeSeriesToAppend,"2018-11-27"),outputData + "/parquet/timeSeries1")

    val t2 = readParquet(spark, outputData + "/parquet/timeSeries1")

    t2.count

    val newT2 = getLatestData(t2)
    newT2.show

    val timeSeries3ToAppend = timeSeries3.except(newT2)
    timeSeries3ToAppend.show()

    writeParquet(addExtrationTime(timeSeries3ToAppend,"2018-11-28"),outputData + "/parquet/timeSeries1")

    val t3 = readParquet(spark, outputData + "/parquet/timeSeries1")
    t3.sort("date").show()
    t3.count()

    val newT3 = getLatestData(t3)
    newT3.show
    timeSeries3.except(newT3).count



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

  def getLatestData(df :DataFrame) : DataFrame = {
    df.groupBy("date").agg(last("value")).withColumnRenamed("last(value, false)","value").drop("extract_time")
  }

  def readParquet(spark: SparkSession,path : String): DataFrame = {
    spark.read.parquet(path).select("date","value","extract_time").sort("date")
  }

  def writeParquet(df :DataFrame, path: String) {
    df.withColumn("month",month(column("date"))).withColumn("year",year(column("date"))).withColumn("day",dayofmonth(column("date"))).
      write.partitionBy("year","month","day").mode(SaveMode.Append).parquet(path)
  }

  def addExtrationTime(df :DataFrame, exDate : String): DataFrame = {
    df.withColumn("extract_time",to_date(lit(exDate)))
  }

  def main(args: Array[String]) {



    val master = "local[*]"

    //val sc = new SparkContext(new SparkConf().setAppName("Scala Spark example").setMaster("local[*]"))

    val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()



    process(spark )

    spark.close()

  }

}
