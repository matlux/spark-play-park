package net.matlux.report

import net.matlux.report.Core.{Providers, genCat, genType, providerType}
import net.matlux.report.Generic._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object RateSetter {

  // Regexes
  val TAXABLE_CASH_BACK_REGEX_EXTRACT = "TaxableCashBack|RateSetter\\scash\\sback"


  // RateSetter Types or categories of transactions
  val RS_CANCELLATION_TYPE = "Cancellation of order"    // opposite or "Lend order"
  val RS_LOAN_PART_TYPE = "Lend order"                  // order ready to be matched. opposite or "Cancellation of order", money committed

  //  val RS_LOAN_OFFER_TYPE = null

  val RS_TRANSFERIN_TYPE = "Bank transfer"
  val RS_TRANSFERIN_CARD_TYPE = "Card payment processed"
  val RS_WITHDRAWAL_TYPE = "Next Day Money Withdrawal request"
  val RS_MONEY_MOVED_TO_OTHER_AC = "Money moved to your other RS Account"

  val RS_FEE_TYPE = "RateSetter lender fee"
  //  val RS_FINAL_FEE_TYPE = null

  // REPAYMENT
  val RS_PRINCIPAL_REPAYMENT_TYPE = "Monthly repayment"
  val RS_EARLY_PRINCIPAL_REPAYMENT_TYPE = "Repaid loan capital"
  val RS_SELLOUT_TYPE = "RepaymentSellOut"
  val RS_PARTIAL_SELLOUT_TYPE = "PartialSelloutRepayment"

  //  val RS_PRINCIPAL_RECOVERY_TYPE = null

  val RS_INTEREST_REPAYMENT_TYPE = "Interest"
  val RS_EARLY_INTEREST_REPAYMENT_TYPE= "Repaid loan interest"
  val RS_INTEREST_SELLOUT_TYPE = "Sellout interest outstanding"
  val RS_TAXABLE_CASH_BACK = "TaxableCashBack"

  val RsTypes2GenericTypes = Map(
    // LOAN
    RS_CANCELLATION_TYPE -> Map(GENERIC_CANCELLATION_TYPE -> Map(EXTRACT_REGEX -> RS_CANCELLATION_TYPE)),
    RS_LOAN_PART_TYPE -> Map(GENERIC_LOAN_PART_TYPE -> Map(EXTRACT_REGEX -> RS_LOAN_PART_TYPE)),

    // MISC
    //        RS_LOAN_OFFER_TYPE -> Map(GENERIC_LOAN_OFFER_TYPE -> Map()),

    // TRANSFER
    RS_TRANSFERIN_TYPE -> Map(GENERIC_TRANSFERIN_TYPE -> Map(EXTRACT_REGEX -> RS_TRANSFERIN_TYPE)),
    RS_TRANSFERIN_CARD_TYPE -> Map(GENERIC_TRANSFERIN_CARD_TYPE -> Map(EXTRACT_REGEX -> RS_TRANSFERIN_CARD_TYPE)),
    RS_WITHDRAWAL_TYPE -> Map(GENERIC_WITHDRAWAL_TYPE -> Map(EXTRACT_REGEX -> RS_WITHDRAWAL_TYPE)),
    RS_MONEY_MOVED_TO_OTHER_AC -> Map(GENERIC_WITHDRAWAL_TYPE -> Map(EXTRACT_REGEX -> RS_MONEY_MOVED_TO_OTHER_AC)),

    // FEE
    RS_FEE_TYPE -> Map(GENERIC_FEE_TYPE -> Map(EXTRACT_REGEX -> RS_FEE_TYPE)),
    //        RS_FINAL_FEE_TYPE -> Map(GENERIC_FINAL_FEE_TYPE2 -> Map()),

    // REPAYMENT
    RS_PRINCIPAL_REPAYMENT_TYPE -> Map(GENERIC_PRINCIPAL_REPAYMENT_TYPE -> Map(EXTRACT_REGEX -> RS_PRINCIPAL_REPAYMENT_TYPE)),
    RS_EARLY_PRINCIPAL_REPAYMENT_TYPE -> Map(GENERIC_EARLY_PRINCIPAL_REPAYMENT_TYPE -> Map(EXTRACT_REGEX -> RS_EARLY_PRINCIPAL_REPAYMENT_TYPE)),
    RS_SELLOUT_TYPE -> Map(GENERIC_SELLOUT_TYPE -> Map(EXTRACT_REGEX -> RS_SELLOUT_TYPE)),

    // RECOVERY
    RS_PARTIAL_SELLOUT_TYPE -> Map(GENERIC_PARTIAL_SELLOUT_TYPE -> Map(EXTRACT_REGEX -> RS_PARTIAL_SELLOUT_TYPE)),
    //        RS_PRINCIPAL_RECOVERY_TYPE -> Map(GENERIC_PRINCIPAL_RECOVERY_TYPE -> Map()),

    // INTEREST
    RS_INTEREST_REPAYMENT_TYPE -> Map(GENERIC_INTEREST_REPAYMENT_TYPE -> Map(EXTRACT_REGEX -> RS_INTEREST_REPAYMENT_TYPE)),
    RS_EARLY_INTEREST_REPAYMENT_TYPE -> Map(GENERIC_EARLY_INTEREST_REPAYMENT_TYPE -> Map(EXTRACT_REGEX -> RS_EARLY_INTEREST_REPAYMENT_TYPE)),
    RS_INTEREST_SELLOUT_TYPE -> Map(GENERIC_INTEREST_SELLOUT_TYPE -> Map(EXTRACT_REGEX -> RS_INTEREST_SELLOUT_TYPE)),
    RS_TAXABLE_CASH_BACK -> Map(GENERIC_INTEREST_SELLOUT_TYPE -> Map(EXTRACT_REGEX -> TAXABLE_CASH_BACK_REGEX_EXTRACT))

  )
  val RsTypes = RsTypes2GenericTypes.keys.toList



  def cleanData(df : DataFrame) = {
    val df2 = df.
      withColumn("type", genType(Providers.RATESETTER)).
      withColumn("cat", genCat(Providers.RATESETTER)).
      withColumn("month",month(column("date"))).withColumn("year",year(column("date")))


    df2
  }
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


}
