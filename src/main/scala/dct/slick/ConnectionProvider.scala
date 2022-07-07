package dct.slick

import org.postgresql.PGConnection
import java.sql.Connection
import scala.util.Try
import akka.stream.alpakka.slick.scaladsl.SlickSession
import slick.jdbc.hikaricp.HikariCPJdbcDataSource

trait ConnectionProvider {
  def acquireBase(): Try[Connection]
  def acquire(): Try[PGConnection]
  def release(exOpt: Option[Throwable]): Unit
}

/**
 * PostgreSQL JDBC [[PGConnection]] provider.
 */
object ConnectionProvider {
  def apply()
           (implicit slickSession: SlickSession): ConnectionProvider =

    new ConnectionProvider{
      private val pool = slickSession.db.source.asInstanceOf[HikariCPJdbcDataSource]
      private var conn: Connection = _
      def acquireBase(): Try[Connection] = Try{
        if (conn == null || conn.isClosed)
          conn = pool.createConnection()
        conn
      }
      def acquire(): Try[PGConnection] = acquireBase().map(_.unwrap(classOf[PGConnection]))
      def release(exOpt: Option[Throwable]): Unit = conn.close()
    }
}