package dct

import akka.actor.ActorSystem
import akka.stream.alpakka.slick.javadsl.SlickSession

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import akka.stream.scaladsl.RunnableGraph
import dct.slick.ConnectionProvider
import dct.spark.Logger

abstract class DataCopyStreamPattern[V <: AnyVal, S <: RunnableGraph[Seq[Future[V]]]]
  extends Logger {

  implicit def sys: ActorSystem
  implicit def ex: ExecutionContext
  val matValuesAggregator: Seq[V] => V

  def rollback(stage: Int): Unit
  def beforeStreaming(): (Seq[S], V)
  def afterStreaming(): Unit

  val executionStages: Map[Int, String] = Map(
    (0, "stream exception"),
    (1, "materialized value mismatch"),
    (2, "before streaming"),
    (3, "after streaming"),
  )

  def runStreaming(): Unit = {
    val (streams, expectedMatValue) = beforeStreaming()
      Future.
        sequence(streams.flatMap(stream => stream.run())).
        map(matValuesAggregator).
        onComplete {
          case Success(value) =>
            if (value == expectedMatValue) {
              logInfo(s"""Streaming returned ${value.toString} as materialized value (total rows copied).""")
              afterStreaming()
            }
            else {
              logError(s"""Streaming returned an unexpected ${value.toString} materialized value. Expected: $expectedMatValue""")
              rollback(1)
            }
          case Failure(ex) =>
            logError(s"Streaming failed due to: ${ex.getMessage}")
            rollback(0)
        }
  } // end of execute method

}

abstract class ParquetToPGStreamPattern(sqlTable: String,
                                        parquetPath: String,
                                        adfMapping: Option[String],
                                        jdbcURL: Option[String])
  extends DataCopyStreamPattern[Long, RunnableGraph[Seq[Future[Long]]]] {

  override def sys: ActorSystem = dct.akkastream.system
  override def ex: ExecutionContext = dct.akkastream.executionContext
  implicit val session: SlickSession = {
    import dct.slick._
    SlickSession.forConfig(
      jdbcURL.
        map(url => forURL(url)).
        orElse(Option(defaultDBConfig)).
        get
    )
  }
  val provider: ConnectionProvider = ConnectionProvider()
  override val matValuesAggregator: Seq[Long] => Long = _.sum

}