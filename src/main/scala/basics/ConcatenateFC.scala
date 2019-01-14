package basics


import java.io.File

import net.matlux.core.Show
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import net.matlux.report.Generic._
import net.matlux.report.FundingCircle._
import net.matlux.report.RateSetter._
import net.matlux.report.Core._
import org.apache.spark.sql.expressions.Window


// https://p2pblog.co.uk/uk-p2p-self-assessment-taxes/

object ConcatenateFC {

  val opts = Map("header" -> "true",
    "dateFormat" -> "yyyy-MM-dd",
    "inferSchema" -> "true")

  val inputData = "/home/mathieu/Dropbox/Finance/investment-transactions/fundingCircleMonthly"
  val outputData = "/home/mathieu/Dropbox/Finance/investment-transactions/"







  Show(FcTypes2GenericTypes)






  Show(FcTypes2Regex)
  FcTypes2GenericCats.keys

  FcTypes2GenericTypes
  Show(GenericType2Category)

  val category = List(LOAN_OFFER_REGEX_EXTRACT,
    TRANSFERIN_REGEX_EXTRACT,
    WITHDRAWAL_REGEX_EXTRACT,
    LOAN_PART_REGEX_EXTRACT,
    SERVICING_FEE_REGEX_EXTRACT,
    SERVICING_FEE_REGEX_EXTRACT2,
    PRINCIPAL_REPAYMENT_REGEX_EXTRACT,
    INTEREST_REPAYMENT_REGEX_EXTRACT,
    EARLY_PRINCIPAL_REPAYMENT_REGEX_EXTRACT,
    EARLY_INTEREST_REPAYMENT_REGEX_EXTRACT,
    PRINCIPAL_RECOVERY_REGEX_EXTRACT
  )
  category.length
  FcTypes2GenericTypes.keys.size

  val rateSetterCategory = List("Cancellation of order",
    "Bank transfer",
    "RateSetter lender fee",
    "Repaid loan capital",
    "Lend order",
    "Card payment processed",
    "Interest",
    "Next Day Money Withdrawal request",
    "Sellout interest outstanding",
    "PartialSelloutRepayment",
    "RepaymentSellOut",
    "Monthly repayment",
    "Repaid loan interest")



  val KEY_TRANSACTION = "11922194"

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  FcTypes2Regex



  def expr(myCols: Set[String], allCols: Set[String]) = {
    allCols.toList.map(x => x match {
      case x if myCols.contains(x) => col(x)
      case _ => lit(null).as(x)
    }).sortBy(c1 => c1.toString())
  }

  def concatenate(spark : SparkSession): Unit = {

    val decimalType: DecimalType = DataTypes.createDecimalType(15, 2)

    val customSchema = StructType(Array(
      StructField("Date", DateType, true),
      StructField("Description", StringType, true),
      StructField("Paid In", decimalType, true),
      StructField("Paid Out", decimalType, true)))

    val listFiles = getListOfFiles(new File(inputData), List("csv")).filter(f => f.getName.matches("Matlux_funding-circles_.*")).sorted

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
    val df00 = dfs.reduceLeft((acc, df) => acc.union(df)).sort(asc("Date"), desc("Paid In"))
    //val dfs = listFiles.map(f => spark.read.options(opts).csv(f.getCanonicalPath))
    //val df = spark.read.options(opts).csv(inputData + "/Matlux_rate-setter_LenderTransactions_all_2017-07-31.csv")

    val optsrs = Map("header" -> "true",
      "dateFormat" -> "dd/MM/yyyy",
      "inferSchema" -> "true")
    val customSchemaRs = StructType(Array(
      StructField("Date", DateType, true),
      StructField("Market", StringType, true),
      StructField("Type", StringType, true),
      StructField("Item", StringType, true),
      StructField("Amount", decimalType, true),
      StructField("Capital", decimalType, true),
      StructField("Interest", decimalType, true),
      StructField("Fee", decimalType, true)
    ))
    //val ratesetterFile = "/../Matlux_rate-setter_LenderTransactions_all_2014-03-24_2018-11-29.csv"
    val ratesetterFile = "/../Mathieu_rate-setter_classic_all_2016-07-19_2019-01-10.csv"
    val dfrs0 = spark.read.format("csv").options(optsrs).schema(customSchemaRs).csv(inputData + ratesetterFile).withColumnRenamed("Type","RsType")


    val df0File = outputData + f"/Matlux_funding-circles_test_${dateRange.head}_${dateRange.last}.cvs"
    val df0 = spark.read.format("csv").options(opts).schema(customSchema).csv(df0File)

    //dfrs0.select(col("Type")).distinct.show(50,false)
    dfrs0.show(50, false)
    dfrs0.groupBy(col("Type")).agg(count(col("Type")), first("Amount")).show(50, false)

    dfrs0.printSchema()
    dfrs0.filter(col("Amount").rlike("1502.88")).sort(asc("Date"), desc("Amount")).show(500, false)
    dfrs0.filter(col("Type").rlike("Cancellation of order")).show(500, false)

    dfrs0.filter(col("Type").rlike("Bank transfer")).show(500, false)
    dfrs0.filter(col("Type").rlike("RepaymentSellOut")).show(500, false)
    dfrs0.filter(col("Type").rlike("Card payment processed")).show(500, false)
    dfrs0.filter(col("Type").rlike("PartialSelloutRepayment")).show(500, false)
    dfrs0.filter(col("Type").rlike("Monthly repayment")).show(500, false)
    dfrs0.filter(col("Type").rlike("Repaid loan capital")).show(500, false)
    dfrs0.filter(col("Type").rlike("PartialSelloutRepayment")).show(500, false)
    dfrs0.filter(col("Item").rlike("C365366711357")).show(500, false)
    dfrs0.filter(col("Item").rlike("C341518375920")).show(500, false)

    val (isCorrectrs, missingElementsrs) = validateCategorisation(Providers.RATESETTER,dfrs0)
    if (!isCorrectrs) missingElementsrs.show(50, false)
    if (isCorrectrs) println("correct")

    df0.sort(asc("Date"), desc("Paid In"), desc("Paid Out")).show(50, false)
    df0.count()

    df0.filter(col("Description").rlike("11922194")).sort(asc("Date"), desc("Paid In")).show(500, false)
    df0.filter(col("Description").rlike(SERVICING_FEE_REGEX_EXTRACT2)).sort(asc("Date"), desc("Paid In")).show(7000, false)
    df0.filter(col("Description").rlike(SERVICING_FEE_REGEX_EXTRACT2)).count
    df0.filter(col("Description").rlike("Interest repayment")).count
    df0.filter(col("Description").rlike(LOAN_OFFER_REGEX_EXTRACT)).sort(asc("Date"), desc("Paid In")).show(500, false)
    df0.filter(col("Description").rlike(TRANSFERIN_REGEX_EXTRACT)).sort(asc("Date"), desc("Paid In")).show(500, false)
    df0.filter(col("Description").rlike(WITHDRAWAL_REGEX_EXTRACT)).sort(asc("Date"), desc("Paid In")).show(500, false)
    df0.filter(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)).sort(asc("Date"), desc("Paid In")).show(500, false)
    df0.filter(col("Description").rlike(SERVICING_FEE_REGEX_EXTRACT)).sort(asc("Date"), desc("Paid In")).show(500, false)
    df0.filter(col("Description").rlike(PRINCIPAL_REPAYMENT_REGEX_EXTRACT)).sort(asc("Date"), desc("Paid In")).show(500, false)
    df0.filter(col("Description").rlike(INTEREST_REPAYMENT_REGEX_EXTRACT)).sort(asc("Date"), desc("Paid In")).show(500, false)


    category.map(cat => df0.filter(col("Description").rlike(cat)).count).sum



    val dfcat = category.map(cat => df0.filter(col("Description").rlike(cat))).reduceLeft((acc, df) => acc.union(df))
    dfcat.count

    df0.except(dfcat).sort(asc("Date"), desc("Paid In")).show(500, false)

    df0.filter(col("Description").rlike("Loan offer")).agg(sum("Paid Out")).show(500, false)

    val (isCorrect, missingElements) = validateCategorisation(Providers.FC,df0)
    if (!isCorrect) missingElements.show(50, false)
    if (isCorrect) println("correct")

    //val df = df0.withColumn("Type",when(column("Description").rlike(LOAN_PART_REGEX_EXTRACT)),lit("loan"))



    FcTypes2GenericTypes(FC_LOAN_PART_TYPE).map(_._1).head
    FcTypes2GenericCats(FC_TRANSFERIN_TYPE)
    FcTypes2Regex

    provider2genType(Providers.FC,FC_LOAN_PART_TYPE)
    FcTypes

//    def fillinType(f: String => String): Column = {
//      val cs : Column = FcTypes.tail.foldLeft(when(pred(FcTypes2Regex(FcTypes.head)), lit(f(FcTypes.head)))){(acc, t) =>
//        acc.when(pred(FcTypes2Regex(t)), lit(t))}
//      cs
//    }

    def addLoanPartCols(df : DataFrame) = {
      df.withColumn("Loan Part ID", when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
        , regexp_replace(col("Description"), LOAN_PART_REGEX_EXTRACT, "$1"))).
        withColumn("Principal", when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
          , regexp_replace(col("Description"), LOAN_PART_REGEX_EXTRACT, "$2"))).
        withColumn("Interest", when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
          , regexp_replace(col("Description"), LOAN_PART_REGEX_EXTRACT, "$3"))).
        withColumn("Delta", when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
          , regexp_replace(col("Description"), LOAN_PART_REGEX_EXTRACT, "$4"))).withColumn("Fee", when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
        , regexp_replace(col("Description"), LOAN_PART_REGEX_EXTRACT, "$5")))
    }

    val df = df0
     //addLoanPartCols(df).
     val df2 = df.//addLoanPartCols(df).
      withColumn("FC type", providerType(Providers.FC)).
      withColumn("type", genType(Providers.FC)).
      withColumn("cat", genCat(Providers.FC)).
       withColumn("Amount",when(column("Paid Out").isNotNull,lit(0).minus(column("Paid Out"))).
         when(column("Paid In").isNotNull,column("Paid In"))).
       withColumn("month",month(column("date"))).withColumn("year",year(column("date")))

    //df2.count()}}
    df2.sort(asc("Date"), desc("Paid In")).show(50, false)
    df2.select(col("type")).distinct().show(50, false)
    df2.select(col("cat")).distinct().show(50, false)

    val df3 = //addLoanPartCols(df).
      dfrs0.
        withColumn("type", genType(Providers.RATESETTER)).
        withColumn("cat", genCat(Providers.RATESETTER)).
        withColumn("month",month(column("date"))).withColumn("year",year(column("date")))

    //df2.count()}}
    df3.show(50, false)


    val df5 = df0.withColumn("month",month(column("date"))).withColumn("year",year(column("date")))
    //val df = spark.read.parquet("/home/mathieu/datashare/hdfs/parquet/test3")

    //val df0 = dfrs0.filter(column("date").gt(lit("2018-04-05"))).show

    val df4 = df2
    df4.filter(column("year")==="2014").groupBy("year","month","type").sum("amount","capital","interest","fee").sort("year","month").show()
    df4.filter(column("year")==="2014").groupBy("year","month","type").sum("amount").sort("year","month").show()
    df4.filter(column("year")==="2014").groupBy("year","month").agg(sum("amount")).sort("year","month").show()
    df4.filter(column("year")==="2014").groupBy("year","month").pivot("type").agg(sum("amount")).sort("year","month").show()
    df4.groupBy("year","month").pivot("cat").agg(sum("amount")).sort("year","month").show()
    df3.groupBy("year","month").pivot("cat").agg(sum("amount")).sort("year","month").show()
    df4.groupBy("year","month").pivot("cat",List("Bank transfer","Interest","RateSetter lender fee")).agg(sum("amount")).sort("year","month").show()


    //val wSpec2 = Window.partitionBy("year","month").orderBy("year","month").rowsBetween(Long.MinValue, 0)
    val wSpec2 = Window.orderBy("year","month").rowsBetween(Long.MinValue, 0)
    val pivotedReport = df4.groupBy("year","month").pivot("cat",List(GENERIC_TRANSFER_CATEGORY,GENERIC_INTEREST_CATEGORY,GENERIC_FEE_CATEGORY)).agg(sum("amount")).sort("year","month")

    val finalReport = pivotedReport.withColumn("cum BT",sum(pivotedReport(GENERIC_TRANSFER_CATEGORY)).over(wSpec2)).
      withColumn("cum interest",sum(pivotedReport(GENERIC_INTEREST_CATEGORY)).over(wSpec2)).
      withColumn("cum fee",sum(pivotedReport(GENERIC_FEE_CATEGORY)).over(wSpec2)).
      select("year","month",GENERIC_TRANSFER_CATEGORY,"cum BT",GENERIC_INTEREST_CATEGORY,"cum interest",GENERIC_FEE_CATEGORY,"cum fee")

    finalReport.show(50,false)


  }

  def main(args: Array[String]) {



    val master = "local[*]"

    //val sc = new SparkContext(new SparkConf().setAppName("Scala Spark example").setMaster("local[*]"))

    val spark = SparkSession.builder.master("local[*]").appName("Simple Application").getOrCreate()



    concatenate(spark )

    spark.close()

  }

}
