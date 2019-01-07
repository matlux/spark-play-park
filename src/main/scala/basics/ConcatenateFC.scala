package basics


import java.io.File

import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


// https://p2pblog.co.uk/uk-p2p-self-assessment-taxes/

object ConcatenateFC {

  val opts = Map("header" -> "true",
    "dateFormat" -> "yyyy-MM-dd",
    "inferSchema" -> "true")

  val inputData = "/home/mathieu/Dropbox/Finance/investment-transactions/fundingCircleMonthly"
  val outputData = "/home/mathieu/Dropbox/Finance/investment-transactions/"

  // Funding Circle Regexes
  val LOAN_OFFER_REGEX_EXTRACT = "Loan offer on (.+) - (.+)"
  val TRANSFERIN_REGEX_EXTRACT = "EPDQ ID: (.+) - TRANSFERIN ORDERID: (.+)"
  val WITHDRAWAL_REGEX_EXTRACT = "FC Len Withdrawal"
  val LOAN_PART_REGEX_EXTRACT = "Loan Part ID (.+) : Principal (.+), Interest (.+), Delta (.+), Fee (.+)"
  val SERVICING_FEE_REGEX_EXTRACT = "Servicing fee for Loan ID N/A; Loan Part ID (.+); Investor ID (.+)"
  val SERVICING_FEE_REGEX_EXTRACT2 = "Servicing fee for loan part (.+)"
  val PRINCIPAL_REPAYMENT_REGEX_EXTRACT = "Principal repayment for loan part (.+)"
  val INTEREST_REPAYMENT_REGEX_EXTRACT = "Interest repayment for loan part (.+)"
  val EARLY_PRINCIPAL_REPAYMENT_REGEX_EXTRACT = "Early principal repayment for loan part (.+)"
  val EARLY_INTEREST_REPAYMENT_REGEX_EXTRACT = "Early interest repayment for loan part (.+)"
  val PRINCIPAL_RECOVERY_REGEX_EXTRACT = "Principal recovery repayment for loan part (.+)"

  // Funding Circle Types or categories of transactions
  val LOAN_OFFER_TYPE = "Loan offer"
  val TRANSFERIN_TYPE = "TRANSFER IN"
  val WITHDRAWAL_TYPE = "Withdrawal"
  val LOAN_PART_TYPE = "Loan Part"
  val FEE_TYPE = "Servicing fee"
  val FINAL_FEE_TYPE2 = "Servicing fee final"
  val PRINCIPAL_REPAYMENT_TYPE = "Principal repayment"
  val INTEREST_REPAYMENT_TYPE = "Interest repayment"
  val EARLY_PRINCIPAL_REPAYMENT_TYPE = "Early principal repayment"
  val EARLY_INTEREST_REPAYMENT_TYPE= "Early interest repayment"
  val PRINCIPAL_RECOVERY_TYPE = "Principal recovery"

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

  // RateSetter Types or categories of transactions
  val RS_CANCELLATION_TYPE = "Cancellation of order"
  val RS_LOAN_OFFER_TYPE = null
  val RS_TRANSFERIN_TYPE = "Bank transfer"
  val RS_TRANSFERIN_CARD_TYPE = "Card payment processed"
  val RS_WITHDRAWAL_TYPE = "Next Day Money Withdrawal request"
  val RS_LOAN_PART_TYPE = "Lend order"
  val RS_FEE_TYPE = "RateSetter lender fee"
  val RS_FINAL_FEE_TYPE = null
  val RS_PRINCIPAL_REPAYMENT_TYPE = "Monthly repayment"
  val RS_INTEREST_REPAYMENT_TYPE = "Interest"
  val RS_EARLY_PRINCIPAL_REPAYMENT_TYPE = "Repaid loan capital"
  val RS_EARLY_INTEREST_REPAYMENT_TYPE= "Repaid loan interest"
  val RS_PRINCIPAL_RECOVERY_TYPE = null
  val RS_SELLOUT_TYPE = "PartialSelloutRepayment"
  val RS_PARTIAL_SELLOUT_TYPE = "RepaymentSellOut"
  val RS_INTEREST_SELLOUT_TYPE = "Sellout interest outstanding"


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

  rateSetterCategory.length

  val FcCategories2RsCategories = Map(

  )

  // providers
  val RATESETTER = "Ratesetter"
  val FC = "Funding Circle"


  val listOfProviders = List()

  // agnostic event types
  val GENERIC_CANCELLATION_TYPE = "Cancellation of order" // opposite or "Lend order"
  val GENERIC_LOAN_PART_TYPE = "Lend order"              // order ready to be matched. opposite or "Cancellation of order"

  val GENERIC_LOAN_OFFER_TYPE = null

  val GENERIC_TRANSFERIN_TYPE = "Bank transfer"
  val GENERIC_TRANSFERIN_CARD_TYPE = "Card payment processed"
  val GENERIC_WITHDRAWAL_TYPE = "Next Day Money Withdrawal request"

  val GENERIC_FEE_TYPE = "RateSetter lender fee"
  val GENERIC_FINAL_FEE_TYPE2 = null

  val GENERIC_PRINCIPAL_REPAYMENT_TYPE = "Monthly repayment"
  val GENERIC_EARLY_PRINCIPAL_REPAYMENT_TYPE = "Repaid loan capital"
  val GENERIC_SELLOUT_TYPE = "PartialSelloutRepayment"
  val GENERIC_PARTIAL_SELLOUT_TYPE = "RepaymentSellOut"

  val GENERIC_PRINCIPAL_RECOVERY_TYPE = null

  val GENERIC_INTEREST_REPAYMENT_TYPE = "Interest"
  val GENERIC_EARLY_INTEREST_REPAYMENT_TYPE= "Repaid loan interest"
  val GENERIC_INTEREST_SELLOUT_TYPE = "Sellout interest outstanding"

  
  val genericEventTypes = Map(
    GENERIC_CANCELLATION_TYPE -> Map(
      RATESETTER -> Some(RS_CANCELLATION_TYPE),
      FC -> None
    ),
    GENERIC_LOAN_OFFER_TYPE -> Map(
      RATESETTER -> None,
      FC -> Some(LOAN_OFFER_TYPE)
    ),
    GENERIC_LOAN_OFFER_TYPE -> Map(
      RATESETTER -> None,
      FC -> Some(LOAN_OFFER_TYPE)
    )

  )

  // agnostic categories types
  val GENERIC_CANCELLATION_CATEGORY = "Cancellation of order" // opposite or "Lend order"
  val GENERIC_LOAN_PART_CATEGORY = "Lend order"              // order ready to be matched. opposite or "Cancellation of order"

  val GENERIC_LOAN_OFFER_CATEGORY = null

  val GENERIC_TRANSFERIN_CATEGORY = "Bank transfer"
  val GENERIC_TRANSFERIN_CARD_CATEGORY = "Card payment processed"
  val GENERIC_WITHDRAWAL_CATEGORY = "Next Day Money Withdrawal request"

  val GENERIC_FEE_CATEGORY = "RateSetter lender fee"
  val GENERIC_FINAL_FEE_CATEGORY2 = null

  val GENERIC_PRINCIPAL_REPAYMENT_CATEGORY = "Monthly repayment"
  val GENERIC_EARLY_PRINCIPAL_REPAYMENT_CATEGORY = "Repaid loan capital"
  val GENERIC_SELLOUT_CATEGORY = "PartialSelloutRepayment"
  val GENERIC_PARTIAL_SELLOUT_CATEGORY = "RepaymentSellOut"

  val GENERIC_PRINCIPAL_RECOVERY_CATEGORY = null

  val GENERIC_INTEREST_REPAYMENT_CATEGORY = "Interest"
  val GENERIC_EARLY_INTEREST_REPAYMENT_CATEGORY = "Repaid loan interest"
  val GENERIC_INTEREST_SELLOUT_CATEGORY = "Sellout interest outstanding"



  val KEY_TRANSACTION = "11922194"

  def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  def validateCategorisation(df : DataFrame) = {
    val dfcat = category.map(cat => df.filter(col("Description").rlike(cat))).reduceLeft((acc,df) => acc.union(df))
    dfcat.count == df.count

    (dfcat.count == df.count,df.except(dfcat).sort(asc("Date"),desc("Paid In")))
  }

  def expr(myCols: Set[String], allCols: Set[String]) = {
    allCols.toList.map(x => x match {
      case x if myCols.contains(x) => col(x)
      case _ => lit(null).as(x)
    }).sortBy(c1 => c1.toString())
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
    val df0 = dfs.reduceLeft((acc,df) => acc.union(df)).sort(asc("Date"),desc("Paid In"))
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
    val dfrs0 = spark.read.format("csv").options(optsrs).schema(customSchemaRs).csv(inputData + "/../Matlux_rate-setter_LenderTransactions_all_2014-03-24_2018-11-29.csv")


    val df0File = outputData + f"/Matlux_funding-circles_test_${dateRange.head}_${dateRange.last}.cvs"
    //val df0 = spark.read.format("csv").options(opts).schema(customSchema).csv(df0File)

    //dfrs0.select(col("Type")).distinct.show(50,false)
    dfrs0.show(50,false)
    dfrs0.groupBy(col("Type")).agg(count(col("Type")),first("Amount")).show(50,false)

    dfrs0.printSchema()
    dfrs0.filter(col("Amount").rlike("1502.88")).sort(asc("Date"),desc("Amount")).show(500,false)
    dfrs0.filter(col("Type").rlike("Cancellation of order")).show(500,false)

    dfrs0.filter(col("Type").rlike("Bank transfer")).show(500,false)
    dfrs0.filter(col("Type").rlike("RepaymentSellOut")).show(500,false)
    dfrs0.filter(col("Type").rlike("Card payment processed")).show(500,false)
    dfrs0.filter(col("Type").rlike("PartialSelloutRepayment")).show(500,false)
    dfrs0.filter(col("Type").rlike("Monthly repayment")).show(500,false)
    dfrs0.filter(col("Type").rlike("Repaid loan capital")).show(500,false)
    dfrs0.filter(col("Type").rlike("PartialSelloutRepayment")).show(500,false)
    dfrs0.filter(col("Item").rlike("C365366711357")).show(500,false)
    dfrs0.filter(col("Item").rlike("C341518375920")).show(500,false)

    df0.sort(asc("Date"),desc("Paid In"),desc("Paid Out")).show(50,false)
    df0.count()

    df0.filter(col("Description").rlike("11922194")).sort(asc("Date"),desc("Paid In")).show(500,false)
    df0.filter(col("Description").rlike(SERVICING_FEE_REGEX_EXTRACT2)).sort(asc("Date"),desc("Paid In")).show(7000,false)
    df0.filter(col("Description").rlike(SERVICING_FEE_REGEX_EXTRACT2)).count
    df0.filter(col("Description").rlike("Interest repayment")).count
    df0.filter(col("Description").rlike(LOAN_OFFER_REGEX_EXTRACT)).sort(asc("Date"),desc("Paid In")).show(500,false)
    df0.filter(col("Description").rlike(TRANSFERIN_REGEX_EXTRACT)).sort(asc("Date"),desc("Paid In")).show(500,false)
    df0.filter(col("Description").rlike(WITHDRAWAL_REGEX_EXTRACT)).sort(asc("Date"),desc("Paid In")).show(500,false)
    df0.filter(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)).sort(asc("Date"),desc("Paid In")).show(500,false)
    df0.filter(col("Description").rlike(SERVICING_FEE_REGEX_EXTRACT)).sort(asc("Date"),desc("Paid In")).show(500,false)
    df0.filter(col("Description").rlike(PRINCIPAL_REPAYMENT_REGEX_EXTRACT)).sort(asc("Date"),desc("Paid In")).show(500,false)
    df0.filter(col("Description").rlike(INTEREST_REPAYMENT_REGEX_EXTRACT)).sort(asc("Date"),desc("Paid In")).show(500,false)



    category.map(cat => df0.filter(col("Description").rlike(cat)).count).sum


    val dfcat = category.map(cat => df0.filter(col("Description").rlike(cat))).reduceLeft((acc,df) => acc.union(df))
      dfcat.count

    df0.except(dfcat).sort(asc("Date"),desc("Paid In")).show(500,false)

    df0.filter(col("Description").rlike("Loan offer")).agg(sum("Paid Out")).show(500,false)

    val (isCorrect,missingElements) =  validateCategorisation(df0)
    if(!isCorrect) missingElements.show(50,false)
    if(isCorrect) println("correct")

    //val df = df0.withColumn("Type",when(column("Description").rlike(LOAN_PART_REGEX_EXTRACT)),lit("loan"))

    val df = df0
    val df2 = df.withColumn("Loan Part ID",when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
      ,regexp_replace(col("Description"),LOAN_PART_REGEX_EXTRACT,"$1")) ).
        withColumn("Principal",when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
          ,regexp_replace(col("Description"),LOAN_PART_REGEX_EXTRACT,"$2")) ).withColumn("Interest",when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
      ,regexp_replace(col("Description"),LOAN_PART_REGEX_EXTRACT,"$3")) ).withColumn("Delta",when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
      ,regexp_replace(col("Description"),LOAN_PART_REGEX_EXTRACT,"$4")) ).withColumn("Fee",when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
      ,regexp_replace(col("Description"),LOAN_PART_REGEX_EXTRACT,"$5")) ).withColumn("Type",when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
      ,lit("Loan Part")) )

    //df2.count()
    df2.sort(asc("Date"),desc("Paid In")).show(50,false)


    val df3 = df.withColumn("Interest",when(col("Description").rlike(LOAN_PART_REGEX_EXTRACT)
        ,regexp_replace(col("Description"),LOAN_PART_REGEX_EXTRACT,"$3")) ).sort(asc("Date"),desc("Paid In"))
    df3.select().show(50,false)

    val cols2 = df2.columns.toSet
    val cols3 = df3.columns.toSet

    df2.select(expr(cols2, cols2 ++ cols3):_*).show(50,false)
    df2.select(expr(cols2, cols2 ++ cols3):_*).union(df3.select(expr(cols3, cols2 ++ cols3):_*)).show(50,false)

    //val df = dfs(0)
    //df.coalesce(1).write .option("header", "true").csv(outputData + f"/Matlux_funding-circles_test_${dateRange.head}_${dateRange.last}.cvs")


    System.out.println("========== Print Schema ============")
    df0.printSchema()
    System.out.println("========== Print Data ==============")
    df.show(50,false)
    System.out.println("========== Print Data df3 ==============")
    df3.show(50,false)
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
