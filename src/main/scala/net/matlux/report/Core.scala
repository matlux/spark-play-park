package net.matlux.report

import net.matlux.core.Show
import net.matlux.report.RateSetter.{RsTypes, RsTypes2GenericTypes}
import net.matlux.report.FundingCircle.{FcTypes, FcTypes2GenericTypes}
import net.matlux.report.Generic._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object Core {

  val RsTypes2Regex = for {
    genTypeInfoMap <- RsTypes2GenericTypes
    genTypeInfo <- genTypeInfoMap._2
  } yield(genTypeInfoMap._1 -> genTypeInfo._2(EXTRACT_REGEX) )


  val RsTypes2GenericCats = for {
    genTypeInfoMap <- RsTypes2GenericTypes
    genTypeInfo <- genTypeInfoMap._2
  } yield(genTypeInfoMap._1 -> GenericType2Category(genTypeInfo._1) )



  val FcTypes2Regex = for {
    genTypeInfoMap <- FcTypes2GenericTypes
    genTypeInfo <- genTypeInfoMap._2
  } yield(genTypeInfoMap._1 -> genTypeInfo._2(EXTRACT_REGEX) )

  val FcTypes2GenericCats = for {
    genTypeInfoMap <- FcTypes2GenericTypes
    genTypeInfo <- genTypeInfoMap._2
  } yield(genTypeInfoMap._1 -> GenericType2Category(genTypeInfo._1) )


  // providers

  object Providers extends Enumeration {
    type Provider = Value
    val RATESETTER, FC = Value
  }


  def providerFunctions(provider : Providers.Provider) = {
    val provider2genType : (Map[String,Map[String,Map[String,String]]],Map[String,String],Map[String,String],String) = provider match{
      case Providers.FC => (FcTypes2GenericTypes,FcTypes2GenericCats,FcTypes2Regex,"Description")
      case Providers.RATESETTER => (RsTypes2GenericTypes,RsTypes2GenericCats,RsTypes2Regex,"RsType")
    }
    provider2genType
  }

  def provider2genType(provider : Providers.Provider,providerType : String ) = {
    val provider2genType2 : Map[String,Map[String,Map[String,String]]] = provider match{
      case Providers.FC => FcTypes2GenericTypes
      case Providers.RATESETTER => RsTypes2GenericTypes
    }
    provider2genType2(providerType).map(_._1).head
  }
  def provider2genCats(provider : Providers.Provider,providerType : String ) = {
    val provider2genType2 : Map[String,String] = provider match{
      case Providers.FC => FcTypes2GenericCats
      case Providers.RATESETTER => RsTypes2GenericCats
    }
    provider2genType2(providerType)
  }
  def validateCategorisation(providerType : Providers.Provider,df : DataFrame) = {
    val (providerTypes2GenericTypes,_,providerTypes2Regex,col2FilterWith) = providerFunctions(providerType)
    val dfcat = providerTypes2GenericTypes.keys.map(cat => df.filter(col(col2FilterWith).rlike(providerTypes2Regex(cat)))).reduceLeft((acc,df) => acc.union(df))
    dfcat.count == df.count

    (dfcat.count == df.count,df.except(dfcat).sort(asc("Date")))
  }

  def fillinType(providerTypes: List[String],providerType2Regex :Map[String,String],c : String)(f: String => String): Column = {
    val cs : Column = providerTypes.tail.foldLeft(when(col(c).rlike(providerType2Regex(providerTypes.head)), lit(f(providerTypes.head)))){(acc, t) =>
      acc.when(col(c).rlike(providerType2Regex(t)), lit(f(t)))}
    cs
  }
  //Providers
  def getFillInTypeFct(providerType : Providers.Provider): (String => String) => Column = {
    providerType match {
      case Providers.FC => fillinType(FcTypes,FcTypes2Regex,"Description")
      case Providers.RATESETTER => fillinType(RsTypes,RsTypes2Regex,"RsType")
    }
  }


  def providerType(providerType : Providers.Provider): Column = {
    val f: (String => String) => Column = getFillInTypeFct(providerType)
    f(identity)
  }
  def genType(providerType : Providers.Provider): Column = {
    val f : (String => String) => Column = getFillInTypeFct(providerType)
    f(genType => provider2genType(providerType,genType))
  }
  def genCat(providerType : Providers.Provider): Column = {
    getFillInTypeFct(providerType).apply(genType => provider2genCats(providerType,genType))
  }


  def generateReport1(df4 : DataFrame, startDateEx :String, endDateEx :String) = {
    val wSpec2 = Window.orderBy("year","month").rowsBetween(Long.MinValue, 0)
    val pivotedReport = df4.filter(column("date").gt(lit(startDateEx))).
      filter(column("date").lt(lit(endDateEx))).
      groupBy("year","month").
      pivot("cat",List(GENERIC_TRANSFER_CATEGORY,GENERIC_INTEREST_CATEGORY,GENERIC_FEE_CATEGORY,GENERIC_PRINCIPAL_RECOVERY_CATEGORY)).
      agg(sum("amount")).
      sort("year","month")

    val finalReport = pivotedReport.
      withColumn("cum BT",sum(pivotedReport(GENERIC_TRANSFER_CATEGORY)).over(wSpec2)).
      withColumn("cum interest",sum(pivotedReport(GENERIC_INTEREST_CATEGORY)).over(wSpec2)).
      withColumn("cum fee",sum(pivotedReport(GENERIC_FEE_CATEGORY)).over(wSpec2)).
      withColumn("cum recovery",sum(pivotedReport(GENERIC_PRINCIPAL_RECOVERY_CATEGORY)).over(wSpec2)).
      withColumn("sum return",col("cum interest").plus(col("cum fee"))).//.plus(col("cum recovery"))
      select("year","month",GENERIC_TRANSFER_CATEGORY,"cum BT",GENERIC_INTEREST_CATEGORY,"cum interest",GENERIC_FEE_CATEGORY,"cum fee",
      GENERIC_PRINCIPAL_RECOVERY_CATEGORY,"cum recovery","sum return")

    finalReport

  }

  val opts = Map("header" -> "true",
    "dateFormat" -> "yyyy-MM-dd",
    "inferSchema" -> "true")

}
