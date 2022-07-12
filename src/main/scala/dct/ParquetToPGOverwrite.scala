package dct

import scala.concurrent.Future
import dct.spark.SparkPGSQLUtils.{changeTableSchema, createTable, dropTable, getSchemaOption, renameTable, tableExists}
import org.apache.spark.sql.types.StructType

/**
 * [[ParquetToPGStream]] implementation for "Overwrite target table" scenario.
 * @param targetTable Target postgreSQL table name.
 * @param temporaryTable Temporary target postgreSQL table name.
 * @param parquetPath hdfs parquet directory or file path.
 * @param adfMapping Optional ADF Mapping json file path. If no mapping provided, full parquet schema will be used.
 * @param jdbcURL Optional JDBC driver URL. By default DATABASE_URL environment variable will be used.
 */
class ParquetToPGOverwrite(targetTable: String,
                           temporaryTable: String,
                           parquetPath: String,
                           adfMapping: Option[String],
                           jdbcURL: Option[String])
  extends ParquetToPGStream(targetTable, temporaryTable, parquetPath, adfMapping, jdbcURL) {

  override def doBeforeStreaming(): Unit =
    try {
      if (!tableExists(targetTable)) {
        logError(s"""Table $targetTable does not exist. Please use \"Create\" mode""")
        throw new StreamInitializationException(s"Table $targetTable does not exist")
      }

      val targetTableSchema = getSchemaOption(targetTable).getOrElse(new StructType())
      val sourceTableSchema = sparkTable.schema

      import dct.spark.StructTypeExtra
      if (targetTableSchema !=== sourceTableSchema) {
        logError(s"Target and source schemas mismatch. Diff: ${sourceTableSchema.diff(targetTableSchema)}")
        throw new StreamInitializationException(s"Target and source schemas mismatch")
      }
      createTable(temporaryTable, Option(targetTableSchema))
      provider.release(None)
    } catch {
      case e: Exception =>
        throw new StreamInitializationException(e.getMessage, e.getCause)
    }

  override def doRecover(): PartialFunction[Throwable, Future[Unit]] = {
    case _: RenameTempToTargetException => Future{
      renameTable(targetSchema + "." + tempTableName, targetTableName)
      provider.release(None)}
    case _: StreamWrappingException => dropTempTableRecovery
    case _: StreamException => dropTempTableRecovery
  }

  override def doAfterStreaming(): Unit =
    try {
      dropTable(targetTable)
      try {
        changeTableSchema(tempTableName, tempSchema, targetSchema)
        renameTable(targetSchema + "." + tempTableName, targetTableName)
      } catch { case e: Exception =>
        throw new RenameTempToTargetException(e.getMessage, e.getCause)
      }
      provider.release(None)
    } catch {
      case e: RenameTempToTargetException => throw e
      case e: Exception => throw new StreamWrappingException(e.getMessage, e.getCause)
    }


  private def dropTempTableRecovery: Future[Unit] = Future{
    dropTable(temporaryTable)
    provider.release(None)
  }
}