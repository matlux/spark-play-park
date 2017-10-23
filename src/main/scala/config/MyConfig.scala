package config

import com.typesafe.config.{ConfigFactory,Config}
import scala.util.Properties

class MyConfig(envName: Option[String],fileNameOption: Option[String] = None) {

  val rawConfig :Config  = fileNameOption.fold(
    ifEmpty = ConfigFactory.load() )(
    file => ConfigFactory.load(file) )

  val config :Config = envName.fold(rawConfig)(
    rawConfig.getConfig(_).withFallback(rawConfig))



  def getString(name: String): String = {
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }
}
