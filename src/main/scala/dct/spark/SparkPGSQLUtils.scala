package dct.spark

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types.{DataType, StructType}
import java.sql.{Connection, SQLException}
import scala.util.Try

//TODO initiate logger as instance into val
//TODO maximum code reuse from JdbcUtils

object SparkPGSQLUtils extends Logger {

  private val url = "jdbc:postgresql"
  private lazy val dialect: JdbcDialect = JdbcDialects.get(url)

  def insertIntoTable(sourceTableName: String,
                      targetTableName: String)
                     (implicit conn: Connection): Unit = {

    val columnNames = getSchemaOption(targetTableName).
      map(schema => schema.map(f => s"""\"${f.name}\"""").mkString(",")).
      getOrElse{
        logError("Could not get target table schema for insert")
      }

    val sql =
      s"""
         |INSERT INTO $targetTableName ($columnNames)
         |SELECT $columnNames FROM $sourceTableName
         |""".stripMargin
    executeStatement(sql)
  }

  def changeTableSchema(tableName: String,
                        oldSchema: String,
                        newSchema: String)
                       (implicit conn: Connection): Unit = {
    val sql = s"ALTER TABLE $oldSchema.$tableName set schema $newSchema"
    executeStatement(sql)
  }

  def getJdbcType(dt: DataType): JdbcType = JdbcUtils.getJdbcType(dt, dialect)

  /**
   * Execute sql statement.
   * @param timeout see https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
   */
  private def executeStatement(sql: String, timeout: Int = 0)
                              (implicit conn: Connection): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(timeout)
      statement.executeUpdate(sql)
      logInfo(s"""Executed query: $sql""")
    } catch {
      case e: SQLException =>
        logError(s"""Failed to execute query: $sql due to ${e.getErrorCode}: $e.getMessage""".stripMargin)
        throw e
    }
    finally {
      statement.close()
    }
  }

  /**
   * Truncates a table from the JDBC database without side effects.
   */
  def truncateTable(tableName: String)
                   (implicit conn: Connection): Unit =
    executeStatement(dialect.getTruncateQuery(tableName))

  /**
   * Returns true if the table already exists.
   */
  def tableExists(tableName: String)
                 (implicit conn: Connection): Boolean =
    Try {
      val statement = conn.prepareStatement(dialect.getTableExistsQuery(tableName))
      try {
        statement.setQueryTimeout(0)
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess

  def renameTable(oldTable: String, newTable: String)
                 (implicit conn: Connection): Unit =
    executeStatement(dialect.renameTable(oldTable, newTable))

  /**
   * Compute the schema string.
   */
  def schemaString(schema: StructType, caseSensitive: Boolean): String =
    JdbcUtils.schemaString(schema, caseSensitive, url)

  /**
   * Create table with options.
   */
  def createTable(tableName: String,
                  schemaOpt: Option[StructType] = None,
                  extraOptions: String = "")
                 (implicit conn: Connection): Unit = {
    val sql: String = schemaOpt.
      map(schema => schemaString(schema, caseSensitive = true)).
      map(schemaStr => s"""CREATE TABLE $tableName ($schemaStr) $extraOptions""").
      getOrElse(s"""CREATE TABLE $tableName $extraOptions""")
    executeStatement(sql)
  }

  def dropTable(tableName: String)
               (implicit conn: Connection): Unit = {
    try executeStatement(s"DROP TABLE $tableName")
    catch {case _: SQLException =>
      logInfo(s"""Apparently $tableName does not exists.Thus there is nothing to drop""")
    }
  }

  /**
   * Returns the schema if the table already exists.
   */
  def getSchemaOption(tableName: String)
                     (implicit conn: Connection): Option[StructType] = {
    try {
      val sql = dialect.getSchemaQuery(tableName)
      val statement = conn.prepareStatement(sql)
      try {
        statement.setQueryTimeout(0)
        Some(JdbcUtils.getSchema(statement.executeQuery(), dialect))
      } catch {
        case e: SQLException =>
          logWarning(s"""Failed to execute query: $sql due to: $e.getMessage""")
          None
      } finally {
        statement.close()
      }
    } catch {
      case e: SQLException =>
        logWarning(s"""Failed to get schema for $tableName due to: $e.getMessage""")
        None
    }
  }

}
