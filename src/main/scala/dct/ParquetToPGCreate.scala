package dct

import java.sql.Connection
import scala.concurrent.Future
import scala.util.{Failure, Success}
import dct.spark.SparkPGSQLUtils.{createTable, dropTable, tableExists}

/**
 * [[ParquetToPGStream]] implementation for "Create target table" scenario.
 * @param targetTable Target postgreSQL table name.
 * @param parquetPath hdfs parquet directory or file path.
 * @param adfMapping Optional ADF Mapping json file path. If no mapping provided, full parquet schema will be used.
 * @param jdbcURL Optional JDBC driver URL. By default DATABASE_URL environment variable will be used.
 */
class ParquetToPGCreate(targetTable: String,
                        parquetPath: String,
                        adfMapping: Option[String],
                        jdbcURL: Option[String])
  extends ParquetToPGStream(targetTable, targetTable, parquetPath, adfMapping, jdbcURL) {

  override def doBeforeStreaming(): Unit =
    try {
      if (!tableExists(targetTable)) {
        createTable(targetTable, Option(sparkTable.schema))
        provider.release(None)
      } else {
        logError(s"""Table $targetTable already exists. Please use \"Overwrite\" mode""")
        throw new StreamInitializationException(s"Table $targetTable already exists")
      }
  } catch {
      case e: Exception =>
        throw new StreamInitializationException(e.getMessage, e.getCause)
    }

  override def doRecover(): PartialFunction[Throwable, Future[Unit]] = {
    case e: StreamException => dropTableRecovery(e)
    case e: StreamWrappingException => dropTableRecovery(e)
  }

  override def doAfterStreaming(): Unit = ()

  private def dropTableRecovery(e: Throwable): Future[Unit] = Future{
    dropTable(targetTable)
    provider.release(None)
  }

}
