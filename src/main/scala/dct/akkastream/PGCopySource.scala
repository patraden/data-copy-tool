package dct.akkastream

import dct.slick.ConnectionProvider
import dct.spark.Logger
import dct.spark.SparkPGSQLUtils.getSchemaOption
import org.postgresql.copy.CopyOut
import akka.stream.alpakka.slick.scaladsl.SlickSession
import akka.stream.scaladsl.Source
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.util.ByteString
import java.sql.Connection
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * COPY based source which returns count of rows copied in metadata.
 * See also [[https://www.postgresql.org/docs/current/sql-copy.html PostgreSQL COPY]]
 * @param query COPY query.
 * @param session SlickSession object for DB connectivity.
 */
class PGCopySource(query: String)(implicit val session: SlickSession)
  extends GraphStageWithMaterializedValue[SourceShape[ByteString], Future[Long]] {

  private val out = Outlet[ByteString]("PgCopySource.out")
  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val completePromise = Promise[Long]()
    val connectionProvider = ConnectionProvider()

    val stageLogic = new GraphStageLogic(shape) with OutHandler {

      private var copyOut: CopyOut = _
      private var connIsAcq: Boolean = false
      private var rowsCopied: Long = 0

      override def onPull(): Unit = {
        Try {
          if (copyOut == null && !connIsAcq) {
            connIsAcq = true
            val conn = connectionProvider.acquire().get
            copyOut = conn.getCopyAPI.copyOut(query)
          }
          Option(copyOut.readFromCopy())
            .map { bytes =>
              rowsCopied += 1L
              ByteString(bytes)
            }
        } match {
          case Success(Some(elem)) => push(out, elem)
          case Success(None)       => success(rowsCopied)
          case Failure(ex)         => fail(ex)
        }
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        if (copyOut != null && copyOut.isActive) {
          copyOut.cancelCopy()
        }
        success(rowsCopied)
      }

      private def success(bytesCopied: Long): Unit = {
        if (connIsAcq) {
          connIsAcq = false
          connectionProvider.release(None)
        }
        completePromise.trySuccess(bytesCopied)
        completeStage()
      }

      private def fail(ex: Throwable): Unit = {
        if (connIsAcq) {
          connIsAcq = false
          connectionProvider.release(Some(ex))
        }
        completePromise.tryFailure(ex)
        failStage(ex)
      }

      setHandler(out, this)
    }

    stageLogic -> completePromise.future
  }

  override def shape: SourceShape[ByteString] = SourceShape.of(out)
}

object PGCopySource extends Logger {

  /**
   * Constructor which derives full set of columns to copy from.
   * @param tableName full postreSQL table name.
   * @param session SlickSession object for DB connectivity
   * @return
   */
  def apply(tableName: String)
           (implicit session: SlickSession): Source[ByteString, Future[Long]] = {
    val provider = ConnectionProvider()
    implicit val conn: Connection =
      if (provider.acquireBase().isSuccess) provider.acquireBase().get
      else null.asInstanceOf[Connection]
    val columnNames = getSchemaOption(tableName).get.map(f => s"""\"${f.name}\"""")
    provider.release(None)
    this.apply(tableName, columnNames)
  }

  /**
   * Base constructor based on specific subset of source columns.
   * @param tableName full postreSQL table name.
   * @param columnNames [[Seq]] of column to copy from.
   * @param session SlickSession object for DB connectivity
   */
  def apply(tableName: String, columnNames: Seq[String])
           (implicit session: SlickSession): Source[ByteString, Future[Long]] = {
    val attr = Attributes.name(s"${tableName}_PGCOPY_SOURCE") and ActorAttributes.IODispatcher
    val query = s"""COPY (SELECT ${columnNames.mkString(",")} FROM $tableName) TO STDOUT"""
    val source = Source.
      fromGraph(new PGCopySource(query)).
      withAttributes(attr)
    logInfo(
      s"""Built copy source for sql table $tableName with query = $query"""
    )
    source
  }
}
