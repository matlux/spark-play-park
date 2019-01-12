package net.matlux.report

import basics.ConcatenateFC._

object Generic {

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


}
