package dct.akkastream

import dct.slick.ConnectionProvider
import dct.spark.Logger
import org.apache.spark.sql.types.StructType
import org.postgresql.copy.CopyIn
import akka.stream.alpakka.slick.scaladsl.SlickSession
import akka.stream.scaladsl.Sink
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}
import akka.util.ByteString

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * COPY based sink which returns count of rows copied in metadata.
 * See also [[https://www.postgresql.org/docs/current/sql-copy.html PostgreSQL COPY]]
 * @param query COPY query.
 * @param session SlickSession object for DB connectivity.
 * @param maxInitialBufferSize Sink won't start until initial buffer is filled.
 */
class PGCopySink(query: String, maxInitialBufferSize: Long = 0L)
                (implicit val session: SlickSession)
  extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Long]] {

  private val in = Inlet[ByteString]("PgCopySink.in")

  def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val connectionProvider = ConnectionProvider()
    val completePromise = Promise[Long]()
    val stageLogic = new GraphStageLogic(shape) with InHandler {

      private var initialBuffer: ByteString = ByteString.empty
      private var copyIn: CopyIn = _

      override def preStart(): Unit = {
        pull(in)
      }

      private def initConnectionAndWriteBuffer(): Unit = {
        connectionProvider.acquire() match {
          case Success(conn) =>
            try {
              copyIn = conn.getCopyAPI.copyIn(query)
              copyIn.writeToCopy(initialBuffer.toArray, 0, initialBuffer.length)
            } catch {
              case ex: Throwable => fail(ex)
            }
          case Failure(ex) => fail(ex)
        }
      }

      def onPush(): Unit = {
        val buf = grab(in)
        try {
          if (copyIn == null) {
            initialBuffer = initialBuffer ++ buf
            if (initialBuffer.size >= maxInitialBufferSize) {
              initConnectionAndWriteBuffer()
            }
          } else {
            copyIn.writeToCopy(buf.toArray, 0, buf.length)
          }
          pull(in)
        } catch {
          case ex: Throwable => fail(ex)
        }
      }

      override def onUpstreamFinish(): Unit = {
        if (copyIn == null && initialBuffer.isEmpty) success(0)
        else {
          if (copyIn == null) {
            initConnectionAndWriteBuffer()
          }
          Try(copyIn.endCopy()) match {
            case Success(rowsCopied) => success(rowsCopied)
            case Failure(ex) => fail(ex)
          }
        }
      }

      override def onUpstreamFailure(ex: Throwable): Unit = {
        try {
          if (copyIn != null && copyIn.isActive) {
            copyIn.cancelCopy()
          }
        } finally {
          fail(ex)
        }
      }

      private def success(rowsCopied: Long): Unit = {
        if (copyIn != null) {
          connectionProvider.release(None)
        }
        completePromise.trySuccess(rowsCopied)
        completeStage()
      }

      private def fail(ex: Throwable): Unit = {
        if (copyIn != null) {
          connectionProvider.release(Some(ex))
        }
        completePromise.tryFailure(ex)
        failStage(ex)
      }

      setHandler(in, this)
    }

    stageLogic -> completePromise.future
  }

  override def shape: SinkShape[ByteString] = SinkShape.of(in)
}

object PGCopySink extends Logger {

  /**
   * Spark sql schema [[StructType]] based constructor.
   * @param tableName full postreSQL table name.
   * @param schema spark sql schema.
   * @param session SlickSession object for DB connectivity
   * @return
   */
  def apply(tableName: String, schema: StructType)
           (implicit session: SlickSession): Sink[ByteString, Future[Long]] = {
    val columnNames = schema.map(f => s"""\"${f.name}\"""")
    this.apply(tableName, columnNames)
  }

  /**
   * Base constructor.
   * @param tableName full postreSQL table name.
   * @param columnNames [[Seq]] of column to copy into.
   * @param session SlickSession object for DB connectivity
   */
  def apply(tableName: String, columnNames: Seq[String])
           (implicit session: SlickSession): Sink[ByteString, Future[Long]] = {

    val attr = Attributes.name(s"${tableName}_COPY_SINK") and ActorAttributes.IODispatcher
    val query = s"""COPY $tableName (${columnNames.mkString(",")}) FROM STDIN"""
    val snk = Sink.
      fromGraph(new PGCopySink(query)).
      withAttributes(attr)

    logInfo(s"""Built copy sink for table: $tableName with query = $query""")
    snk
  }

}