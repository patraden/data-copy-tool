package dct.akkastreams

import dct.spark.{Logger, SparkPartitionReader}

import scala.concurrent.Future
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.PartitionedFile
import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{ActorAttributes, Attributes, Outlet, Shape, SourceShape}

/**
 * A GraphStage represents a reusable graph stream processing operator defined for [[SparkPartitionReader]].
 * Normally a GraphStage consists of a [[Shape]] which describes its input and output ports and a factory function that
 * creates a [[GraphStageLogic]] which implements the processing logic that ties the ports together.
 * This class specifically implements a [[SourceShape]] with single output and no inputs.
 */
class SparkPartitionReaderSourceShapeGraph(fileReader: SparkPartitionReader)
  extends GraphStage[SourceShape[Row]]{

  val out: Outlet[Row] = Outlet("SparkPartitionReaderSource")

  override protected def initialAttributes: Attributes =
    super.initialAttributes and
      Attributes.name("SparkPartitionReaderSource") and
      ActorAttributes.dispatcher("dct.akka.blocking-io-dispatcher")

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

object SparkPartitionReaderSourceShapeGraph extends Logger {

  /**
   * Returns lazy [[Source]] class object for specific spark [[PartitionedFile]]
   * out of [[SparkPartitionReaderSourceShapeGraph]]
   * @param file input file.
   * @param readerInitializer internal spark reader initializer for [[PartitionedFile]].
   */
  def lazySourceForFile(file: PartitionedFile,
                        readerInitializer: PartitionedFile => SparkPartitionReader
                       ): Source[Row, Future[NotUsed]] = {
    val source = Source.lazySource(() =>
      Source.fromGraph(new SparkPartitionReaderSourceShapeGraph(readerInitializer(file)))
    )
    logInfo(s"Created Akka Streams Source for Partitioned File: $file")
    source
  }

  /**
   * Returns lazy [[Source]] class object for specific spark [[PartitionedFile]]
   * out of akka DSL [[Source.unfoldResource]].
   * @param file input file.
   * @param readerInitializer internal spark reader initializer for [[PartitionedFile]].
   */
  def lazyUnfoldedSourceForFile(file: PartitionedFile,
                                readerInitializer: PartitionedFile => SparkPartitionReader
                               ): Source[Row, Future[NotUsed]] = {
    val source = Source.lazySource(() =>
      Source.unfoldResource[Row, SparkPartitionReader](
        create = () => readerInitializer(file),
        read = reader =>
          if (reader.next()) Some(reader.get())
          else None,
        close = reader => reader.close()
      )
    )
    logInfo(s"Created Akka Streams Source for Partitioned File $file")
    source
  }

}