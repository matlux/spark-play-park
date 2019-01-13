package net.matlux.report

import net.matlux.core.Show
import net.matlux.report.RateSetter.RsTypes2GenericTypes
import net.matlux.report.FundingCircle.FcTypes2GenericTypes
import net.matlux.report.Generic.{EXTRACT_REGEX, GenericType2Category}

object Core {

  val RsTypes2Regex = for {
    genTypeInfoMap <- RsTypes2GenericTypes
    genTypeInfo <- genTypeInfoMap._2
  } yield(genTypeInfoMap._1 -> genTypeInfo._2(EXTRACT_REGEX) )

  RsTypes2Regex
  Show(RsTypes2GenericTypes)

  val RsTypes2GenericCats = for {
    genTypeInfoMap <- RsTypes2GenericTypes
    genTypeInfo <- genTypeInfoMap._2
  } yield(genTypeInfoMap._1 -> GenericType2Category(genTypeInfo._1) )

  RsTypes2Regex
  Show(RsTypes2GenericCats)

  val FcTypes2Regex = for {
    genTypeInfoMap <- FcTypes2GenericTypes
    genTypeInfo <- genTypeInfoMap._2
  } yield(genTypeInfoMap._1 -> genTypeInfo._2(EXTRACT_REGEX) )

  val FcTypes2GenericCats = for {
    genTypeInfoMap <- FcTypes2GenericTypes
    genTypeInfo <- genTypeInfoMap._2
  } yield(genTypeInfoMap._1 -> GenericType2Category(genTypeInfo._1) )

  Show(FcTypes2GenericCats)

  // providers
//  val RATESETTER = "Ratesetter"
//  val FC = "Funding Circle"

  object Providers extends Enumeration {
    type Provider = Value
    val RATESETTER, FC = Value
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



}
