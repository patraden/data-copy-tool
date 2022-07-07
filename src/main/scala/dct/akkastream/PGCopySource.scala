package dct.akkastream

import dct.slick.ConnectionProvider
import akka.stream.alpakka.slick.scaladsl.SlickSession
import akka.stream.scaladsl.Source
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.util.ByteString
import dct.spark.Logger
import dct.spark.SparkPGSQLUtils.getSchemaOption
import org.postgresql.copy.CopyOut

import java.sql.Connection
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

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
              rowsCopied += 1L //bytes.length for original val = bytesCopied
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
   * Spark schema based constructor.
   * @param tableName
   * @param session
   * @return
   */
  def apply(tableName: String)
           (implicit session: SlickSession): Source[ByteString, Future[Long]] = {
    val connTry = ConnectionProvider().acquireBase()
    implicit val conn: Connection = if (connTry.isSuccess) connTry.get else null.asInstanceOf[Connection]
    val columnNames = getSchemaOption(tableName).get.map(f => s"""\"${f.name}\"""")
    this.apply(tableName, columnNames)
  }

  /**
   * Base constructor.
   * @param tableName
   * @param columnNames
   * @param session
   * @return
   */
  def apply(tableName: String, columnNames: Seq[String])
           (implicit session: SlickSession): Source[ByteString, Future[Long]] = {
    val attr = Attributes.name(s"${tableName}_PGCOPY_SOURCE") and ActorAttributes.IODispatcher
    val query = s"""COPY (SELECT ${columnNames.mkString(",")} FROM ${tableName}) TO STDOUT"""
    val source = Source.
      fromGraph(new PGCopySource(query)).
      withAttributes(attr)
    logInfo(
      s"""Built copy source for sql table ${tableName}
         |with query = $query
         |""".stripMargin)
    source
  }
}
