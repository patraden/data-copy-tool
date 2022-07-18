package dct.akkastreams

import dct.slick.ConnectionProvider
import org.postgresql.copy.CopyIn

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import akka.stream.{ActorAttributes, Attributes, Inlet, Shape, SinkShape}
import akka.stream.alpakka.slick.scaladsl.SlickSession
import akka.stream.scaladsl.Sink
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.util.ByteString
import dct.spark.Logger
import org.apache.spark.sql.types.StructType

/**
 * A GraphStage represents a reusable graph stream processing operator defined for Postgres COPY command.
 * Normally a GraphStage consists of a [[Shape]] which describes its input and output ports and a factory function that
 * creates a [[GraphStageLogic]] which implements the processing logic that ties the ports together.
 * This class specifically implements a [[SinkShape]] with single input and no outputs.
 * @see [[https://www.postgresql.org/docs/current/sql-copy.html Postgres COPY command]]
 */
class PostgresCopySinkShapeGraph(query: String,
                                 maxInitialBufferSize: Long = 0L)
                                (implicit val session: SlickSession)
  extends GraphStageWithMaterializedValue[SinkShape[ByteString], Future[Long]] {

  private val in = Inlet[ByteString]("PostgresCopySink.in")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and
      Attributes.name("PostgresCopySink") and
      ActorAttributes.dispatcher("dct.akka.blocking-io-dispatcher")

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

object PostgresCopySinkShapeGraph extends Logger {

  /**
   * Returns [[Sink]] class object out of [[PostgresCopySinkShapeGraph]].
   * @param tableName full target Postgres table name [schema.tableName].
   * @param columnNames target table column subset to COPY into.
   * @param session An "open" Slick database and its database (type) profile.
   */
  def sink(tableName: String,
           columnNames: Seq[String])
           (implicit session: SlickSession): Sink[ByteString, Future[Long]] = {
    val query = s"""COPY $tableName (${columnNames.mkString(",")}) FROM STDIN"""
    val sink = Sink.fromGraph(new PostgresCopySinkShapeGraph(query))
    logInfo(s"Created Akka Streams Sink for query: $query")
    sink
  }

  /**
   * Returns [[Sink]] class object out of [[PostgresCopySinkShapeGraph]].
   * @param tableName full target Postgres table name [schema.tableName].
   * @param schema spark sql dataframe schema.
   * @param session An "open" Slick database and its database (type) profile.
   */
  def sinkForSparkSchema(tableName: String,
                         schema: StructType)
                        (implicit session: SlickSession): Sink[ByteString, Future[Long]] = {
    val columnNames = schema.map(f => s"""\"${f.name}\"""")
    sink(tableName, columnNames)
  }
}