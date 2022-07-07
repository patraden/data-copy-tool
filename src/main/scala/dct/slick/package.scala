package dct

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

package object slick {

  val defaultDBConfig: Config =
    ConfigFactory.load().getConfig("postgres")

  def forURL(url: String): Config =
    defaultDBConfig.withValue("db.properties.url", ConfigValueFactory.fromAnyRef(url))

  def isValidJdbcURL(url: String): Boolean = {
    url.nonEmpty //TODO regex implementation
  }
}
