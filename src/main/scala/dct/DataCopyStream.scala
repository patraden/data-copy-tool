package dct

import akka.actor.ActorSystem
import akka.stream.alpakka.slick.javadsl.SlickSession
import akka.stream.scaladsl.RunnableGraph
import dct.akkastream.SparkParquetTableToPGCopyStream
import dct.json.ADFMapping
import dct.slick.ConnectionProvider
import dct.spark.SparkParquetTable

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Super class for command line copy scenarios end to end.
 * The core in a scenario is a set of akka streams [[RunnableGraph]] which perform copy itself.
 * @tparam V - akka stream materialized value type.
 * @tparam S - akka stream closed shape [[RunnableGraph]] extension.
 */

abstract class DataCopyStream[V, S <: RunnableGraph[Seq[Future[V]]]] extends dct.spark.Logger {
  implicit val sys: ActorSystem
  implicit val ex: ExecutionContext

  val streams: Try[Seq[S]]
  val expectedMetaData: Try[V]
  val matValuesAggregator: Seq[V] => V

  /**
   * Abstract method encapsulating steps to be executed prior to data streaming between Source and Sink.
   * Typically these include validation and preparation of Sink artifacts (Source validation happens silently).
   * It is highly recommended to wrap all potential exceptions that you want to recover with [[StreamInitializationException]].
   * When catch these in [[DataCopyStream.doRecover]] and apply recovery steps in the [[Future]]
   * Example:
   * {{{
   * try {
   *   doSomething()
   * } catch {
   *   case e: Exception => throw new StreamInitializationException(e.getMessage, e.getCause)
   * }
   * }}}
   */
  def doBeforeStreaming(): Unit

  /**
   * Abstract method encapsulating steps to be executed when data streaming between Source and Sink is finished.
   * Typically these should be performed when you need to modify target Sink artifacts (overwrite and append scenarios).
   * It is highly recommended to wrap all potential exceptions that you want to recover with [[StreamWrappingException]].
   * When catch these in [[DataCopyStream.doRecover]] and apply recovery steps in the [[Future]]
   * Example:
   * {{{
   * try {
   *   doSomething()
   * } catch {
   *   case e: Exception => throw new StreamWrappingException(e.getMessage, e.getCause)
   * }
   * }}}
   */
  def doAfterStreaming(): Unit

  /**
   * Roll back steps for three major stages of DataCopyPattern.
   * [[DataCopyStream.doBeforeStreaming]] which normally throws [[StreamInitializationException]].
   * [[DataCopyStream.streaming]] which always throws [[StreamException]].
   * And [[DataCopyStream.doAfterStreaming]] which normally throws [[StreamWrappingException]].
   * @return [[PartialFunction]] defined at subset of exceptions that needs to be recovered.
   */
  def doRecover(): PartialFunction[Throwable, Future[Unit]]

  private def beforeStream(): TransactFuture[Unit] = Future {
    (streams, expectedMetaData) match {
      case (Failure(ex), _) =>
        logError("Akka streams metadata value not initialized")
        throw new StreamInitializationException(ex.getMessage, ex.getCause)
      case (_, Failure(ex)) =>
        logError("Akka streams metadata value not initialized")
        throw new StreamInitializationException(ex.getMessage, ex.getCause)
      case (Success(_), Success(_)) =>
        doBeforeStreaming()
    }
  }.rollbackWith(
    doRecover().
      andThen(
        rb =>
          rb.recover {
            case e: Exception =>
              logError("Recovery failed!!! Remove streaming artifacts manually.")
              throw new StreamRollbackException(e.getMessage, e.getCause)
          }.andThen{ _ =>
            Future.successful(logInfo("Forced termination of the akka Actor system")).
            flatMap(_ => sys.terminate())
          }
        ),
    sys
  )

  private def streaming(): Future[Unit] =
    Future.
      sequence(streams.get.flatMap(_.run())).
      map(matValuesAggregator).
      map{value =>
        if (value == expectedMetaData.get) logInfo(s"Successfully copied $value rows")
        else throw new StreamMetaDataAssertionException(value.toString, expectedMetaData.get.toString)}.
      recover{ case e: Exception =>
        logError("Streaming stage failure")
        throw new StreamException(e.getMessage, e.getCause)
      }

  private def afterStream(): Future[Unit] = Future(doAfterStreaming())

  private[dct] def end2endPattern(): Future[Unit] = for {
    _ <- beforeStream()
    _ <- streaming()
    _ <- afterStream()
    _ <- Future.successful(
      logInfo(s"Data Copy executed successfully in ${System.currentTimeMillis() - sys.startTime} milliseconds")
    ).andThen(_ => sys.terminate())
  } yield ()

  /**
   * Main method to execute end to end data copy streaming.
   */
  def execute(): Unit = end2endPattern().onComplete(_ => sys.terminate())

}

/**
 * [[DataCopyStream]] to copy parquet file to PostgreSQL table.
 * @param targetTable Target postgreSQL table name.
 * @param temporaryTable Temporary target postgreSQL table name.
 * @param parquetPath hdfs parquet directory or file path.
 * @param adfMapping Optional ADF Mapping json file path. If no mapping provided, full parquet schema will be used.
 * @param jdbcURL Optional JDBC driver URL. By default DATABASE_URL environment variable will be used.
 */
abstract class ParquetToPGStream(targetTable: String,
                                 temporaryTable: String,
                                 parquetPath: String,
                                 adfMapping: Option[String],
                                 jdbcURL: Option[String]
                                ) extends DataCopyStream[Long, RunnableGraph[Seq[Future[Long]]]] {

  override implicit val ex: ExecutionContext = dct.akkastream.executionContext
  override implicit val sys: ActorSystem = dct.akkastream.system

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
  implicit def conn: Connection = provider.acquireBase() match {
    case Success(conn) => conn
    case Failure(e) =>
      logError("Failed to establish initial connection to DB")
      throw e
  }

  val (targetSchema, targetTableName) = targetTable.split('.') match {
    case Array(s, t) => (s, t)
    case Array(t) => ("public", t)
  }

  val (tempSchema, tempTableName) = temporaryTable.split('.') match {
    case Array(s, t) => (s, t)
    case Array(t) => ("public", t)
  }

  sys.registerOnTermination(() => session.close())

  override val matValuesAggregator: Seq[Long] => Long = _.sum
  override val expectedMetaData: Try[Long] = Try(sparkTable.totalRowsCount)
  override val streams: Try[Seq[RunnableGraph[Seq[Future[Long]]]]] =
    Try(SparkParquetTableToPGCopyStream(sparkTable).buildStreams())

  lazy val sparkTable: SparkParquetTable =
    new SparkParquetTable(
      temporaryTable,
      Seq(parquetPath),
      adfMapping.flatMap(path => ADFMapping(path).mappingSchemaAsStructType).orElse(None)
    )
}