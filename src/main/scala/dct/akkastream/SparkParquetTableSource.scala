package dct.akkastream

import scala.concurrent.Future
import dct.spark.{Logger, SparkParquetTable, SparkRowReader}
import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{ActorAttributes, Attributes, Outlet, SourceShape}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.PartitionedFile

@InternalApi
private[dct] class SparkParquetTableSource(fileReader: SparkRowReader)
  extends GraphStage[SourceShape[Row]] {

  val out: Outlet[Row] = Outlet("SparkParquetTableSource")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and ActorAttributes.IODispatcher

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
    setHandler(
      out,
      new OutHandler {

        override def onDownstreamFinish(cause: Throwable): Unit = {
          super.onDownstreamFinish(cause)
          fileReader.close()
        }

        override def onPull(): Unit = {
          val record =
            if (fileReader.next()) fileReader.get()
            else null

          Option(record).fold {
            complete(out)
          }(push(out, _))
        }
      }
    )

    override def postStop(): Unit = fileReader.close()
  }
  override def shape: SourceShape[Row] = SourceShape.of(out)
}

private[dct] object SparkParquetTableSource extends Logger {

  /**
   * Advance akka API source constructor.
   */
  private def graphSource(file: PartitionedFile,
                          reader: PartitionedFile => SparkRowReader
                         ): Source[Row, Future[NotUsed]] = {
    val attr = Attributes.name("SparkParquetTableSource")
    val src = Source.lazySource(
      () => Source.fromGraph(new SparkParquetTableSource(reader(file)))
    )
    logInfo(s"""Created source for PartitionedFile: ${file.filePath}[${file.start} + ${file.length}]""")
    src.withAttributes(attr)
  }

  /**
   * Simple akka API source constructor.
   */
  private def unfoldedSource(file: PartitionedFile,
                             reader: PartitionedFile => SparkRowReader
                            ): Source[Row, Future[NotUsed]] = {
    val attr = Attributes.name("SparkTableSource") and ActorAttributes.IODispatcher
    val src = Source.lazySource(
      () => Source.unfoldResource[Row, SparkRowReader](
        create = () => reader(file),
        read = reader =>
          if (reader.next()) Some(reader.get())
          else None,
        close = reader => reader.close()
      )
    )
    logInfo(s"""Created source for PartitionedFile: ${file.filePath}[${file.start} + ${file.length}]""")
    src.withAttributes(attr)
  }

  /**
   * Akka Sources constructor for [[SparkParquetTable]].
   * @return indexed [[Map]] of [[Source]]
   */
  def apply(table: SparkParquetTable): Map[Int, Source[Row, Future[NotUsed]]] =
    (0 until table.parallelism).
      map(index => (index, table.splitFilesFilter(index).
        foldLeft(
          Source.lazySource(() => Source.empty[Row]))(
          (foldedSource, partitionedFile) => {
              val newSource =
                unfoldedSource(partitionedFile, table.rowReader).async
              foldedSource.prependLazy(newSource).async
          }
        ))
      ).toMap

}
