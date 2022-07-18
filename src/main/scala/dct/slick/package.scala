package dct

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

package object slick {

  /**
   * Default database configuration from application.conf.
   * jdbc URL sourced from DATABASE_URL environment variable.
   */
  val defaultDBConfig: Config =
    ConfigFactory.load().getConfig("dct.postgres")

  /**
   * Database configuration with explicit jdbc URL.
   */
  def forURL(url: String): Config =
    defaultDBConfig.withValue("db.properties.url", ConfigValueFactory.fromAnyRef(url))

  /**
   * jdbc url syntax validator.
   */
  def isValidJdbcURL(url: String): Boolean = {
    url.nonEmpty //TODO regex implementation
  }
}
