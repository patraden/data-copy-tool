package dct.spark

import dct.slick.ConnectionProvider
import java.sql.{Connection, ResultSet, SQLException, Statement}
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.StructType

/**
 * Postgres SQl utility methods to create, alter and drop objects in database.
 * Re-uses spark internal utilities for critical and sensitive operations like type conversion.
 */
object SparkPGSQLUtils extends Logger {

  private val UrlPrefix = "jdbc:postgresql"
  private lazy val dialect: JdbcDialect = JdbcDialects.get(UrlPrefix)

  private def schemaString(schema: StructType): String =
    JdbcUtils.schemaString(schema, caseSensitive = true, UrlPrefix)

  /**
   * Database connection resources management template.
   */
  private def withConnectionProvider[T](provider: ConnectionProvider)
                                    (op: Connection => T): T = {
    provider.acquireBase() match {
      case Success(conn) =>
        try op(conn) finally provider.release(None)
      case Failure(e) =>
        logError("Failed to establish connection to DB")
        provider.release(Option(e))
        throw e
    }
  }

  /**
   * SQL query execution template.
   * @param sql query.
   * @param timeout connection timeout.
   * @param prepare function returning [[Statement]] for query.
   * @param exec function returning statement execution result.
   * @param provider [[ConnectionProvider]]
   * @tparam T statement execution result type (e.g. Int for update and ResultSet for query).
   * @tparam S statement type (might be subclass like PreparedStatement).
   */
  private def executeStatement[T, S <: Statement](sql: String, timeout: Int, withLog: Boolean)
                                                 (prepare: (Connection, String) => S)
                                                 (exec: (S, String) => T)
                                                 (implicit provider: ConnectionProvider): T =
    withConnectionProvider(provider){
      conn =>
        val statement = prepare(conn, sql)
        try {
          statement.setQueryTimeout(timeout)
          val res = exec(statement, sql)
          if (withLog)
            logInfo(s"""Executed query: $sql""")
          res
        } catch {
          case e: SQLException =>
            if (withLog)
              logError(s"""Failed to execute query: $sql due to ${e.getErrorCode}: $e.getMessage""".stripMargin)
            throw e
        }
        finally statement.close()
    }

  private def executeUpdate(sql: String, timeout: Int = 0, withLog: Boolean = true)
                           (implicit provider: ConnectionProvider): Int =
    executeStatement(sql, timeout, withLog)
    { (conn, _) => conn.createStatement }
    { (statement, sql) => statement.executeUpdate(sql) }

  private def executeQuery(sql: String, timeout: Int = 0, withLog: Boolean = true)
                           (implicit provider: ConnectionProvider): ResultSet =
    executeStatement(sql, timeout, withLog)
    { (conn, sql) => conn.prepareStatement(sql) }
    { (statement, _) => statement.executeQuery() }

  def changeTableSchema(fullTableName: String, schema: String)
                       (implicit provider: ConnectionProvider): Int = {
    val (oldSchema, tableName) = fullTableName.split('.') match {
      case Array(s, t) => (s, t)
      case Array(t) => ("public", t)
    }
    executeUpdate(s"ALTER TABLE $oldSchema.$tableName set schema $schema")
  }

  def truncateTable(fullTableName: String)
                   (implicit provider: ConnectionProvider): Int =
    executeUpdate(dialect.getTruncateQuery(fullTableName))

  def tableExists(fullTableName: String)
                 (implicit provider: ConnectionProvider): Boolean =
    Try { executeQuery( dialect.getTableExistsQuery(fullTableName), withLog = false) }.isSuccess

  def renameTable(fullTableName: String,
                  newFullTableName: String)
                 (implicit provider: ConnectionProvider): Int = {
    val newTableName = newFullTableName.split('.') match {
      case Array(_, t) => t
      case Array(t) => t
    }
    executeUpdate(dialect.renameTable(fullTableName, newTableName))
  }

  def createTable(fullTableName: String,
                  schemaOpt: Option[StructType] = None,
                  extraOptions: String = "")
                 (implicit provider: ConnectionProvider): Int = {
    val sql: String = schemaOpt.
      map(schema => schemaString(schema)).
      map(schemaStr => s"""CREATE TABLE $fullTableName ($schemaStr) $extraOptions""").
      getOrElse(s"""CREATE TABLE $fullTableName $extraOptions""")
    executeUpdate(sql)
  }

  def dropTable(tableName: String)
               (implicit provider: ConnectionProvider): Int =
    try
      executeUpdate(s"DROP TABLE $tableName")
    catch { case _: SQLException =>
      logWarning(s"""Apparently $tableName does not exists.Thus there is nothing to drop""")
      -1
    }

  def getSchema(tableName: String)
               (implicit provider: ConnectionProvider): StructType = {
    val sql = dialect.getSchemaQuery(tableName)
    executeStatement(sql, timeout = 0, withLog = true)
    { (conn, _) => conn.prepareStatement(sql) }
    { (statement, _) => JdbcUtils.getSchema(statement.executeQuery(), dialect) }
  }

}