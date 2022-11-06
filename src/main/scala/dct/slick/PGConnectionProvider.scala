package dct.slick

import akka.stream.alpakka.slick.scaladsl.SlickSession
import com.typesafe.config.ConfigFactory
import org.postgresql.PGConnection
import slick.jdbc.hikaricp.HikariCPJdbcDataSource
import scala.util.Try

/**
 * Serializable [[PGConnection]] provider for custom spark receiver.
 * @param configPath slick session config path.
 */
case class PGConnectionProvider(configPath: String) {
  private var pool: HikariCPJdbcDataSource = _
  def release(): Unit = if (pool != null || !pool.ds.isClosed) pool.close()
  def acquire(): Try[PGConnection] = Try{
    if (pool == null)
      pool = SlickSession.
        forConfig(ConfigFactory.load().getConfig(configPath)).
        db.
        source.
        asInstanceOf[HikariCPJdbcDataSource]
    pool.createConnection().unwrap(classOf[PGConnection])
  }
}
