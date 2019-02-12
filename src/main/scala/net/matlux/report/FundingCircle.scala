package net.matlux.report

import java.io.File

import basics.ConcatenateFC.inputData
import net.matlux.core.FileUtils.getListOfFiles
import net.matlux.report.Core._
import net.matlux.report.Generic._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object FundingCircle {

  // Funding Circle Regexes
  val LOAN_PART_REGEX_EXTRACT = "Loan Part ID (.+) : Principal (.+), Interest (.+), Delta (.+), Fee (.+)"

  val LOAN_OFFER_REGEX_EXTRACT = "Loan offer on (.+) - (.+)"

  val TRANSFERIN_REGEX_EXTRACT = "EPDQ ID: (.+) - TRANSFERIN ORDERID: (.+)"
  val WITHDRAWAL_REGEX_EXTRACT = "FC Len Withdrawal"

  val SERVICING_FEE_REGEX_EXTRACT = "Servicing fee for Loan ID N/A; Loan Part ID (.+); Investor ID (.+)"
  val SERVICING_FEE_REGEX_EXTRACT2 = "Servicing fee for loan part (.+)"

  val PRINCIPAL_REPAYMENT_REGEX_EXTRACT = "Principal repayment for loan part (.+)"
  val EARLY_PRINCIPAL_REPAYMENT_REGEX_EXTRACT = "Early principal repayment for loan part (.+)"

  val PRINCIPAL_RECOVERY_REGEX_EXTRACT = "Principal recovery repayment for loan part (.+)"

  val INTEREST_REPAYMENT_REGEX_EXTRACT = "Interest repayment for loan part (.+)"
  val EARLY_INTEREST_REPAYMENT_REGEX_EXTRACT = "Early interest repayment for loan part (.+)"



  // Funding Circle Types or categories of transactions
  //val CANCELLATION_TYPE = null
  val FC_LOAN_PART_TYPE = "Loan Part"

  val FC_LOAN_OFFER_TYPE = "Loan offer"

  val FC_TRANSFERIN_TYPE = "TRANSFER IN"
  //val TRANSFERIN_CARD_TYPE = null
  val FC_WITHDRAWAL_TYPE = "Withdrawal"

  val FC_FEE_TYPE = "Servicing fee"
  val FC_FINAL_FEE_TYPE2 = "Servicing fee final"

  // REPAYMENT
  val FC_PRINCIPAL_REPAYMENT_TYPE = "Principal repayment"
  val FC_EARLY_PRINCIPAL_REPAYMENT_TYPE = "Early principal repayment"
  //val SELLOUT_TYPE = null
  //val PARTIAL_SELLOUT_TYPE = null

  val FC_PRINCIPAL_RECOVERY_TYPE = "Principal recovery"

  val FC_INTEREST_REPAYMENT_TYPE = "Interest repayment"
  val FC_EARLY_INTEREST_REPAYMENT_TYPE = "Early interest repayment"



  val FcTypes2GenericTypes = Map(
    // LOAN
    //    CANCELLATION_TYPE -> Map(GENERIC_CANCELLATION_TYPE -> Map()),
    FC_LOAN_PART_TYPE -> Map(GENERIC_LOAN_PART_TYPE -> Map(EXTRACT_REGEX -> LOAN_PART_REGEX_EXTRACT)),

    // MISC
    FC_LOAN_OFFER_TYPE -> Map(GENERIC_LOAN_OFFER_TYPE -> Map(EXTRACT_REGEX -> LOAN_OFFER_REGEX_EXTRACT)),

    // TRANSFER
    FC_TRANSFERIN_TYPE -> Map(GENERIC_TRANSFERIN_TYPE -> Map(EXTRACT_REGEX -> TRANSFERIN_REGEX_EXTRACT)),
    //    TRANSFERIN_CARD_TYPE -> Map(GENERIC_TRANSFERIN_CARD_TYPE -> Map()),
    FC_WITHDRAWAL_TYPE -> Map(GENERIC_WITHDRAWAL_TYPE -> Map(EXTRACT_REGEX -> WITHDRAWAL_REGEX_EXTRACT)),

    // FEE
    FC_FEE_TYPE -> Map(GENERIC_FEE_TYPE -> Map(EXTRACT_REGEX -> SERVICING_FEE_REGEX_EXTRACT)),
    FC_FINAL_FEE_TYPE2 -> Map(GENERIC_FINAL_FEE_TYPE2 -> Map(EXTRACT_REGEX -> SERVICING_FEE_REGEX_EXTRACT2)),

    // REPAYMENT
    FC_PRINCIPAL_REPAYMENT_TYPE -> Map(GENERIC_PRINCIPAL_REPAYMENT_TYPE -> Map(EXTRACT_REGEX -> PRINCIPAL_REPAYMENT_REGEX_EXTRACT)),
    FC_EARLY_PRINCIPAL_REPAYMENT_TYPE -> Map(GENERIC_EARLY_PRINCIPAL_REPAYMENT_TYPE -> Map(EXTRACT_REGEX -> EARLY_PRINCIPAL_REPAYMENT_REGEX_EXTRACT)),
    //    SELLOUT_TYPE -> Map(GENERIC_SELLOUT_TYPE -> Map()),

    // RECOVERY
    //    PARTIAL_SELLOUT_TYPE -> Map(GENERIC_PARTIAL_SELLOUT_TYPE -> Map()),
    FC_PRINCIPAL_RECOVERY_TYPE -> Map(GENERIC_PRINCIPAL_RECOVERY_TYPE -> Map(EXTRACT_REGEX -> PRINCIPAL_RECOVERY_REGEX_EXTRACT)),

    // INTEREST
    FC_INTEREST_REPAYMENT_TYPE -> Map(GENERIC_INTEREST_REPAYMENT_TYPE -> Map(EXTRACT_REGEX -> INTEREST_REPAYMENT_REGEX_EXTRACT)),
    FC_EARLY_INTEREST_REPAYMENT_TYPE -> Map(GENERIC_EARLY_INTEREST_REPAYMENT_TYPE -> Map(EXTRACT_REGEX -> EARLY_INTEREST_REPAYMENT_REGEX_EXTRACT))
  )

  val FcTypes = FcTypes2GenericTypes.keys.toList

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

  def cleanData(df : DataFrame) = {
    val df2 = df.//addLoanPartCols(df).
      withColumn("FC type", providerType(Providers.FC)).
      withColumn("type", genType(Providers.FC)).
      withColumn("cat", genCat(Providers.FC)).
      withColumn("Amount",when(column("Paid Out").isNotNull,lit(0).minus(column("Paid Out"))).
        when(column("Paid In").isNotNull,column("Paid In"))).
      withColumn("month",month(column("date"))).withColumn("year",year(column("date")))

    df2
  }

  val customSchema = StructType(Array(
    StructField("Date", DateType, true),
    StructField("Description", StringType, true),
    StructField("Paid In", decimalType, true),
    StructField("Paid Out", decimalType, true)))


  val listFiles = getListOfFiles(new File(inputData), List("csv")).filter(f => f.getName.matches(matcher)).sorted

  def loadFcMonthlyFiles(spark: SparkSession, matcher : String) = {
    val dateRange = listFiles.flatMap(f =>
      s"${matcher}_\\d{4}-\\d{2}-.._..-..-..\\.csv".r.findFirstMatchIn(f.getName) match {
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
    (df00, dateRange)
  }


}
