package net.matlux.report

import net.matlux.report.Generic._

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

}
