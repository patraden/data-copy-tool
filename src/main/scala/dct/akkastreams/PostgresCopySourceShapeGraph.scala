package dct.akkastreams

import dct.slick.ConnectionProvider
import org.postgresql.copy.CopyOut

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import akka.stream.{ActorAttributes, Attributes, Outlet, Shape, SourceShape}
import akka.stream.alpakka.slick.scaladsl.SlickSession
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.util.ByteString
import dct.spark.Logger
import org.apache.spark.sql.types.StructType

/**
 * A GraphStage represents a reusable graph stream processing operator defined for Postgres COPY command.
 * Normally a GraphStage consists of a [[Shape]] which describes its input and output ports and a factory function that
 * creates a [[GraphStageLogic]] which implements the processing logic that ties the ports together.
 * This class specifically implements a [[SourceShape]] with single output and no inputs.
 * @see [[https://www.postgresql.org/docs/current/sql-copy.html Postgres COPY command]]
 */
class PostgresCopySourceShapeGraph(query: String)
                                  (implicit val session: SlickSession)
  extends GraphStageWithMaterializedValue[SourceShape[ByteString], Future[Long]] {

  private val out = Outlet[ByteString]("PgCopySource.out")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and
      Attributes.name("PostgresCopySource") and
      ActorAttributes.dispatcher("dct.akka.blocking-io-dispatcher")

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

object PostgresCopySourceShapeGraph extends Logger{

  /**
   * Returns lazy [[Source]] class object out of [[PostgresCopySourceShapeGraph]].
   * @param tableName full target Postgres table name [schema.tableName].
   * @param columnNames target table column subset to COPY from.
   * @param session An "open" Slick database and its database (type) profile.
   */
  def lazySourceForTable(tableName: String, columnNames: Seq[String])
                        (implicit session: SlickSession): Source[ByteString, Future[Long]] = {
    val query = s"""COPY (SELECT ${columnNames.mkString(",")} FROM $tableName) TO STDOUT"""
    val source = Source.fromGraph(new PostgresCopySourceShapeGraph(query))
    logInfo(s"Created Akka Streams Source for query: $query")
    source
  }

  /**
   * Returns lazy [[Source]] class object out of [[PostgresCopySourceShapeGraph]].
   * @param tableName full target Postgres table name [schema.tableName].
   * @param schema spark sql dataframe schema.
   * @param session An "open" Slick database and its database (type) profile.
   */
  def lazySourceForSparkSchema(tableName: String,
                               schema: StructType)
                              (implicit session: SlickSession): Source[ByteString, Future[Long]] = {
    val columnNames = schema.map(f => s"""\"${f.name}\"""")
    lazySourceForTable(tableName, columnNames)
  }

}
