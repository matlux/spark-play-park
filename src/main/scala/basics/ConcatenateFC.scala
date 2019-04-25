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
import net.matlux.report.{FundingCircle, RateSetter}
import net.matlux.core.FileUtils.{getListOfFiles}


// https://p2pblog.co.uk/uk-p2p-self-assessment-taxes/

object ConcatenateFC {


  val inputData = "/home/mathieu/Dropbox/Finance/investment-transactions/fundingCircleMonthly"
  val outputData = "/home/mathieu/Dropbox/Finance/investment-transactions/"


  val KEY_TRANSACTION = "11922194"


  def expr(myCols: Set[String], allCols: Set[String]) = {
    allCols.toList.map(x => x match {
      case x if myCols.contains(x) => col(x)
      case _ => lit(null).as(x)
    }).sortBy(c1 => c1.toString())
  }

  def concatenate(spark : SparkSession): Unit = {


    val perso = "Mathieu_funding-circles_classic_(.*)"

    val matlux = "Matlux_funding-circles_(.*)"

    val matcher = matlux

    val (df00, dateRange) = loadFcMonthlyFiles(spark, matcher)

    val optsrs = Map("header" -> "true",
      "dateFormat" -> "dd/MM/yyyy",
      "inferSchema" -> "true")

    //val ratesetterFile = "/../Matlux_rate-setter_LenderTransactions_all_2014-03-24_2018-11-29.csv"
    //val ratesetterFile = "/../Mathieu_rate-setter_classic_all_2016-07-19_2019-01-10.csv"
    val ratesetterFile = "/../Mathieu_rate-setter_classic_all_2016-07-19_2019-01-10.csv"

    val dfrs0 = spark.read.format("csv").options(optsrs).schema(customSchemaRs).csv(inputData + ratesetterFile).withColumnRenamed("Type","RsType")


    val df0File = outputData + f"/Matlux_funding-circle_all_${dateRange.head}_${dateRange.last}.cvs"
    val df0 = spark.read.format("csv").options(opts).schema(customSchema).csv(df0File)

    //val df = dfs(0)
    //
    //df00.coalesce(1).write .option("header", "true").csv(outputData + f"/Matlux_funding-circles_test_${dateRange.head}_${dateRange.last}.cvs")


    //dfrs0.filter(col("Type").rlike("PartialSelloutRepayment")).show(500, false)
    //dfrs0.filter(col("Item").rlike("C365366711357")).show(500, false)
    //dfrs0.filter(col("Item").rlike("C341518375920")).show(500, false)

    val (isCorrectrs, missingElementsrs) = validateCategorisation(Providers.RATESETTER,dfrs0)
    if (!isCorrectrs) missingElementsrs.show(50, false)
    if (isCorrectrs) println("correct")


    //df0.filter(col("Description").rlike(LOAN_OFFER_REGEX_EXTRACT)).sort(asc("Date"), desc("Paid In")).show(500, false)

    val (isCorrect, missingElements) = validateCategorisation(Providers.FC,df00)
    if (!isCorrect) missingElements.show(50, false)
    if (isCorrect) println("correct")


    val df = df00
    df.show
    df.printSchema()
     //addLoanPartCols(df).
     val fcDf = FundingCircle.cleanData(df)

    fcDf.show(50,false)

    val rsDf = RateSetter.cleanData(dfrs0)

    val finalReportFc = generateReport1(fcDf,"2018-03-31","2019-04-01")
    val finalReport = generateReport1(rsDf,"2018-03-31","2019-04-01")
    finalReportFc.show(50,false)
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
