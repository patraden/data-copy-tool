package dct.akkastream

import akka.NotUsed
import akka.stream.ClosedShape
import akka.stream.alpakka.slick.scaladsl.SlickSession
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import dct.spark.SparkParquetTable
import org.apache.spark.sql.Row
import scala.concurrent.Future

class SparkParquetTableToPGCopyStream(val table: SparkParquetTable)
                                     (implicit session: SlickSession) {
  /**
   * Converts [[Row]] to postresql sql-copy compatible akka [[ByteString]]
   * See https://postgrespro.ru/docs/postgresql/14/sql-copy
   */
  private def internalRowToPGCopyByteString(row: Row, extraRowValues: String*): ByteString = {
    val escapeChars = Seq(
      "\\" -> "\\\\", // escape `escape` character is first
      "\b" -> "\\b",
      "\f" -> "\\f",
      "\n" -> "\\n",
      "\r" -> "\\r",
      "\t" -> "\\t",
      "\u0011" -> "\\v"
    )
    def escape: String => String =
      escapeChars.foldLeft[String => String](identity) {
        case (resultFunction, (sFrom, sTo)) =>
          resultFunction.andThen(_.replace(sFrom, sTo))
      }

    val rowSeq =
      if (extraRowValues.isEmpty) row.toSeq
      else row.toSeq ++ extraRowValues

    ByteString(
      rowSeq.map {
        case None | null => """\N"""
        case Some(value) => escape(value.toString)
        case value => escape(value.toString)
      }
        .mkString("", "\t", "\n")
        .getBytes("UTF-8")
    )
  }

  def sparkRowSink(name: String): Sink[Row, Future[Long]] = {
    Flow[Row].
      map(row => internalRowToPGCopyByteString(row)).
      log("sparkRowSink error").
      toMat(PGCopySink(table.tableName, table.schema))(Keep.right)
  }

  val sparkRowSources: Map[Int, Source[Row, Future[NotUsed]]] = SparkParquetTableSource(table)
  val sparkRowSinks: Seq[Sink[Row, Future[Long]]] =
    (0 until SINK_PARALLELISM).
      map(index => sparkRowSink(s"PGCopySink $index"))

  /**
   * https://doc.akka.io/docs/akka/current/stream/stream-graphs.html#constructing-graphs
   * @param src
   * @return
   */
  def singleStream(src: Source[Row, Future[NotUsed]], name: String): RunnableGraph[Seq[Future[Long]]] =
    RunnableGraph.fromGraph( GraphDSL.create(sparkRowSinks) { implicit builder => sinkList =>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val blnc = builder.add(Balance[Row](sinkList.size))
      src ~> blnc
      sinkList.foreach(sink => blnc ~> sink)
      ClosedShape
  }).named(name)

  def buildStreams(): Seq[RunnableGraph[Seq[Future[Long]]]] =
    sparkRowSources.map{
      case (index, src) => singleStream(src, s"PG Copy stream $index")
    }.toSeq

}

object SparkParquetTableToPGCopyStream {

  def apply(table: SparkParquetTable)
           (implicit session: SlickSession) =
    new SparkParquetTableToPGCopyStream(table)

}
