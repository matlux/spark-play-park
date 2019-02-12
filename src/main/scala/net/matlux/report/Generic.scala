package net.matlux.report

import basics.ConcatenateFC._
import org.apache.spark.sql.types.{DataTypes, DecimalType}

object Generic {

  // KEYs
  val EXTRACT_REGEX = "EXTRACT_REGEX"


  // agnostic event types

  // LOAN
  val GENERIC_CANCELLATION_TYPE = "Cancellation of order gen type" // opposite or "Lend order"
  val GENERIC_LOAN_PART_TYPE = "LOAN_PART gen type"              // order ready to be matched. opposite or "Cancellation of order"

  // MISC
  val GENERIC_LOAN_OFFER_TYPE = "LOAN_OFFER gen type"

  // TRANSFER
  val GENERIC_TRANSFERIN_TYPE = "Bank transfer gen type"
  val GENERIC_TRANSFERIN_CARD_TYPE = "Card payment processed gen type"
  val GENERIC_WITHDRAWAL_TYPE = "Next Day Money Withdrawal request gen type"

  // FEE
  val GENERIC_FEE_TYPE = "FEE gen type"
  val GENERIC_FINAL_FEE_TYPE2 = "FINAL_FEE gen type"

  // REPAYMENT
  val GENERIC_PRINCIPAL_REPAYMENT_TYPE = "Monthly PRINCIPAL_REPAYMENT gen type"
  val GENERIC_EARLY_PRINCIPAL_REPAYMENT_TYPE = "EARLY_PRINCIPAL_REPAYMENT gen type"
  val GENERIC_SELLOUT_TYPE = "RepaymentSellOut gen type"
  val GENERIC_PARTIAL_SELLOUT_TYPE = "PartialSelloutRepayment gen type"

  // RECOVERY
  val GENERIC_PRINCIPAL_RECOVERY_TYPE = "PRINCIPAL_RECOVERY gen type"

  // INTEREST
  val GENERIC_INTEREST_REPAYMENT_TYPE = "Interest"
  val GENERIC_EARLY_INTEREST_REPAYMENT_TYPE= "EARLY_INTEREST_REPAYMENT gen type"
  val GENERIC_INTEREST_SELLOUT_TYPE = "INTEREST_SELLOUT gen type"


  // agnostic categories types
  val GENERIC_LOAN_CATEGORY = "LOAN cat"              // order ready to be matched. opposite or "Cancellation of order"
  val GENERIC_MISC_CATEGORY = "MISC cat"
  val GENERIC_TRANSFER_CATEGORY = "TRANSFER cat"
  val GENERIC_FEE_CATEGORY = "FEE cat"
  val GENERIC_REPAYMENT_CATEGORY = "REPAYMENT cat"
  val GENERIC_PRINCIPAL_RECOVERY_CATEGORY = "RECOVERY cat"
  val GENERIC_INTEREST_CATEGORY = "INTEREST cat"



  val genericCategories = Map(
    GENERIC_LOAN_CATEGORY -> Map(
      GENERIC_CANCELLATION_TYPE -> Map(),
      GENERIC_LOAN_PART_TYPE -> Map()
    ),
    GENERIC_MISC_CATEGORY -> Map(
      GENERIC_LOAN_OFFER_TYPE -> Map()
    ),
    GENERIC_TRANSFER_CATEGORY -> Map(
      GENERIC_TRANSFERIN_TYPE -> Map(),
      GENERIC_TRANSFERIN_CARD_TYPE -> Map(),
      GENERIC_WITHDRAWAL_TYPE-> Map()
    ),
    GENERIC_FEE_CATEGORY -> Map(
      GENERIC_FEE_TYPE -> Map(),
      GENERIC_FINAL_FEE_TYPE2 -> Map()
    ),
    GENERIC_REPAYMENT_CATEGORY -> Map(
      GENERIC_PRINCIPAL_REPAYMENT_TYPE -> Map(),
      GENERIC_EARLY_PRINCIPAL_REPAYMENT_TYPE -> Map(),
      GENERIC_SELLOUT_TYPE -> Map(),
      GENERIC_PARTIAL_SELLOUT_TYPE -> Map()
    ),
    GENERIC_PRINCIPAL_RECOVERY_CATEGORY -> Map(
      GENERIC_PRINCIPAL_RECOVERY_TYPE -> Map()
    ),

    GENERIC_INTEREST_CATEGORY -> Map(
      GENERIC_INTEREST_REPAYMENT_TYPE -> Map(),
      GENERIC_EARLY_INTEREST_REPAYMENT_TYPE -> Map(),
      GENERIC_INTEREST_SELLOUT_TYPE -> Map()
    )
  )

  val GenericType2Category = for{
    cat <- genericCategories
    atype <- cat._2
  } yield (atype._1 -> cat._1)



  val decimalType: DecimalType = DataTypes.createDecimalType(15, 2)


}
