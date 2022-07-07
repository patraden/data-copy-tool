package dct

import java.sql.Connection
import scala.concurrent.Future
import akka.stream.scaladsl.RunnableGraph
import dct.akkastream.SparkParquetTableToPGCopyStream
import dct.json.ADFMapping
import dct.spark.SparkPGSQLUtils.{createTable, dropTable, tableExists}
import dct.spark.SparkParquetTable


case class ParquetToPGCopyCreate(sqlTable: String,
                                 parquetPath: String,
                                 adfMapping: Option[String],
                                 jdbcURL: Option[String])
  extends ParquetToPGStreamPattern(sqlTable, parquetPath, adfMapping, jdbcURL) {

  override def beforeStreaming(): (Seq[RunnableGraph[Seq[Future[Long]]]], Long) = {
    logInfo("Validating Source, Sink to initialize streaming.")
    try {
      sys.registerOnTermination(() => session.close())
      implicit val conn: Connection = provider.acquireBase().get
      if (tableExists(sqlTable)) {
        logError(s"Target table name $sqlTable cannot be created as it already exists.")
        sys.terminate()
        System.exit(1)
      }
      val adfMap = adfMapping match {
        case Some(path) => ADFMapping(path).mappingSchema
        case _ => None
      }
      val sparkTable = new SparkParquetTable(sqlTable, Seq(parquetPath), adfMap)
      createTable(sqlTable, Option(sparkTable.schema))
      provider.release(None)
      val expectedRows = sparkTable.totalRowsCount
      val (streams, expectedMatValue) = (SparkParquetTableToPGCopyStream(sparkTable).buildStreams, expectedRows)
      logInfo("Streaming initialized")
      (streams, expectedMatValue)
    } catch {
      case e: Exception =>
        logError(s"""Failure while validating Source, Sink and initializing copy stream""")
        sys.terminate()
        throw e
    }

  }

  def afterStreaming(): Unit = sys.terminate()

  def rollback(stage: Int): Unit = {
    logInfo(s"""Trying to roll back execution stage $stage:\"${executionStages(stage)}\"""")
    try {
      implicit val conn: Connection = provider.acquireBase().get
      val rollBackBody = () => {
        if (tableExists(sqlTable)) dropTable(sqlTable)
        logInfo(s"""Rolled back execution stage $stage:\"${executionStages(stage)}\"""")
      }
      stage match {
        case 0 => rollBackBody()
        case 1 => rollBackBody()
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

