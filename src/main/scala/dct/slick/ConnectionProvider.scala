package dct.slick

import org.postgresql.PGConnection
import java.sql.Connection
import scala.util.Try
import akka.stream.alpakka.slick.scaladsl.SlickSession
import slick.jdbc.hikaricp.HikariCPJdbcDataSource

/**
 * Generic connection provider capable to provide base Java and alternative PosgresSQL JDBC connections.
 * All DB Sources and Sinks will require this interface.
 */
trait ConnectionProvider {
  def acquireBase(): Try[Connection]
  def acquire(): Try[PGConnection]
  def release(exOpt: Option[Throwable]): Unit
}

/**
 * PostgreSQL JDBC connection provider implementation.
 */
object ConnectionProvider {

  def apply()(implicit slickSession: SlickSession): ConnectionProvider =
    new ConnectionProvider{
      private var conn: Connection = _
      def acquireBase(): Try[Connection] =
        Try{
          if (conn == null || conn.isClosed)
            conn = slickSession.db.source.asInstanceOf[HikariCPJdbcDataSource].createConnection()
          conn
        }
      def acquire(): Try[PGConnection] = acquireBase().map(_.unwrap(classOf[PGConnection]))
      def release(exOpt: Option[Throwable]): Unit =
        if (conn != null || !conn.isClosed) conn.close()
    }

}