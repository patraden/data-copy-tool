package dct.akkastreams

import scala.concurrent.Future
import akka.NotUsed
import akka.stream.alpakka.slick.scaladsl.SlickSession
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import dct.spark.{SparkParquetTable, SparkPartitionReader}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType

/**
 * Parquet to PostgresSQL stream with [[ClonedSinkFanOutStream]] shape.
 * @param src parquet table source which return spark [[Row]] class.
 * @param snk Postres COPY sink which takes [[ByteString]] and return count of rows copied.
 * @param numOuts number of cloned sinks
 * @param extraRowValues extra columns in [[ByteString]] as result of appends to original [[Row]]
 */

class SparkParquetToPGTableStream(
  src: Source[Row, Future[NotUsed]],
  snk: Sink[ByteString, Future[Long]],
  numOuts: Int,
  extraRowValues: String*)
  extends ClonedSinkFanOutStream[Row, ByteString, Future[NotUsed], Future[Long]](numOuts) {

  override def source: Source[Row, Future[NotUsed]] = src
  override def sink: Sink[ByteString, Future[Long]] = snk
  override def typeConverter: Row => ByteString = (row: Row) => {
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
}

object SparkParquetToPGTableStream {

  /**
   * Single stream constructor sourcing [[Seq]] of spark [[PartitionedFile]].
   * For parquet file format normally [[PartitionedFile]] represents a single row group when read by spark parquet reader.
   * However sometimes it might result in an empty reader (this is an internal design of spark parquet reader).
   * This constructor should not be used alone as it sources just a part of a full parquet file.
   * Additionally it requires size assertion of [[Row]] returned by reader and [[Seq]] of columnNames
   * which should be performed outside of this constructor.
   * @param partitionedFiles sequence of files to be sourced.
   * @param readerInitializer reader initializer which returns [[SparkPartitionReader]] for input file.
   * @param targetTable full target Postgres table name [schema.tableName].
   * @param schema spark sql dataframe schema.
   * @param numOuts number of cloned sinks
   * @param extraRowValues extra columns in [[ByteString]] as result of appends to original [[Row]]
   * @param session An "open" Slick database and its database (type) profile.
   */
  private def apply(partitionedFiles: Seq[PartitionedFile],
                    readerInitializer: PartitionedFile => SparkPartitionReader,
                    targetTable: String,
                    schema: StructType,
                    numOuts: Int,
                    extraRowValues: String*)
                   (implicit session: SlickSession): SparkParquetToPGTableStream = {
    val src = partitionedFiles.
      foldLeft(
        Source.lazySource(() => Source.empty[Row])
      )( (foldedSource, partitionedFile) => {
        val newSource = SparkPartitionReaderSourceShapeGraph.
          lazyUnfoldedSourceForFile(partitionedFile, readerInitializer)
        foldedSource.prependLazy(newSource)
      }
    )
    val snk = PostgresCopySinkShapeGraph.sinkForSparkSchema(targetTable, schema)
    new SparkParquetToPGTableStream(src, snk, numOuts, extraRowValues: _*)
  }

  /**
   * Creates a [[Seq]] of [[SparkParquetToPGTableStream]].
   * Size of the resulted sequence equals to [[SparkParquetTable.parallelism]].
   * Logical data split is defined in [[SparkParquetTable.splitFilesFilter]].
   * Resulted streams will be configured to use max potential of dedicated connection pool from db session.
   * @param sparkParquet input spark parquet table.
   * @param targetTable full target Postgres table name [schema.tableName].
   * @param session An "open" Slick database and its database (type) profile.
   */
  def apply(sparkParquet: SparkParquetTable,
            targetTable: String)
           (implicit session: SlickSession): Seq[SparkParquetToPGTableStream] = {

    val parallelism = sparkParquet.parallelism
    val numThreads: Int = session.db.source.maxConnections.getOrElse(parallelism)
    val numOuts = (numThreads - 1)/parallelism

    (0 until parallelism).
      map(index => this.apply(
        sparkParquet.splitFilesFilter(index),
        sparkParquet.rowReader,
        targetTable,
        sparkParquet.schema,
        numOuts
      ))
  }
}
