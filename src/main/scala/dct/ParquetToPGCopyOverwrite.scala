package dct

import dct.akkastream.SparkParquetTableToPGCopyStream
import dct.spark.SparkParquetTable
import dct.json.ADFMapping
import dct.spark.SparkPGSQLUtils.{
  changeTableSchema,
  createTable,
  dropTable,
  getSchemaOption,
  renameTable,
  tableExists
}
import scala.concurrent.Future
import org.apache.spark.sql.types.StructType
import akka.stream.scaladsl.RunnableGraph
import java.sql.Connection

case class ParquetToPGCopyOverwrite(sqlTable: String,
                                 parquetPath: String,
                                 adfMapping: Option[String],
                                 jdbcURL: Option[String])
  extends ParquetToPGStreamPattern(sqlTable, parquetPath, adfMapping, jdbcURL) {

  private val tmpTableName = sqlTable.replace(".", "_") + "_dct_tmp"
  private val tmpSchema = "public"
  private val tmpTable = tmpSchema + "." + tmpTableName

  override def beforeStreaming(): (Seq[RunnableGraph[Seq[Future[Long]]]], Long) = {
    logInfo("Validating Source, Sink to initialize streaming.")
    try {
      import dct.spark.StructTypeExtra
      sys.registerOnTermination(() => session.close())
      implicit val conn: Connection = provider.acquireBase().get
      if (!tableExists(sqlTable)) {
        logError(s"""Target table name $sqlTable does not exists. Run with \"create\" mode""")
        sys.terminate()
        System.exit(1)
      }
      val adfMap = adfMapping match {
        case Some(path) => ADFMapping(path).mappingSchema
        case _ => None
      }
      val targetSchema = getSchemaOption(sqlTable).getOrElse(new StructType())
      val sparkTable = new SparkParquetTable(tmpTable, Seq(parquetPath), adfMap)
      val sourceSchema = sparkTable.schema
      if (targetSchema !=== sourceSchema) {
        logError(s"Target and source schemas mismatch. Diff: ${sourceSchema.diff(targetSchema)}")
        sys.terminate()
        System.exit(1)
      }
      createTable(tmpTable, Option(targetSchema))
      provider.release(None)
      val (streams, expectedMatValue) = (SparkParquetTableToPGCopyStream(sparkTable).buildStreams, sparkTable.totalRowsCount)
      logInfo("Streaming initialized")
      (streams, expectedMatValue)
    } catch {
      case e: Exception =>
        logError(s"""Failure while validating Source, Sink and initializing copy stream""")
        rollback(2)
        throw e
    }
  }

  override def afterStreaming(): Unit = {
    val (schema, tableName) = sqlTable.split('.') match {
      case Array(s, n) => (s, n)
      case Array(n) => ("public", n)
      case _ => ("public", null.asInstanceOf[String])
    }
    try {
      implicit val conn: Connection = provider.acquireBase().get
      changeTableSchema(tmpTableName, tmpSchema, schema)
      dropTable(sqlTable)
      renameTable(schema + "." + tmpTableName, tableName)
      sys.terminate()
    } catch {
      case e: Exception =>
        rollback(3)
    }
  }

  override def rollback(stage: Int): Unit = {
    logInfo(s"""Trying to roll back execution stage $stage:\"${executionStages(stage)}\"""")
    try {
      implicit val conn: Connection = provider.acquireBase().get
      val rollBackStream = () => {
        if (tableExists(tmpTableName)) dropTable(tmpTableName)
        logInfo(s"""Rolled back execution stage $stage:\"${executionStages(stage)}\"""")
      }
      stage match {
        case 0 => rollBackStream()
        case 1 => rollBackStream()
        case 2 => rollBackStream()
        case 3 => sys.terminate(); ???
        case _ => logError("Unknown rollback stage.")
      }
      provider.release(None)
    } catch {
      case e: Exception =>
        logError(s"""Failure while rolling back execution $stage ${e.getMessage}""".stripMargin)
        sys.terminate()
        throw e
    }
  }
}