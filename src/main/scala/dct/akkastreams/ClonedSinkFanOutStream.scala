package dct.akkastreams

import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.stream.ClosedShape

/*
 * Stream shape schematics for ClonedSinkFanOutStream(3)
 *          +--------------------------------------------------------------+
 *          | ClosedShape                             +------------------+ |
 *          |                                      --->      Sink        -----> Data
 *          |                     +----------+   -/   |   [In, SnkMat]   | |
 *          |                     |          | -/     +------------------+ |
 *          | +---------------+   |          -/                            |
 *          | |    Source     |   |          |        +------------------+ |
 *  Data -->  | [Out, SrcMat] ----> Balancer --------->      Sink        -----> Data
 *          | +---------------+   |          |        |   [In, SnkMat]   | |
 *          |                     |          -\       +------------------+ |
 *          |                     |          | -\                          |
 *          |                     +----------+   -\   +------------------+ |
 *          |                                      --->      Sink        -----> Data
 *          |                                         |   [In, SnkMat]   | |
 *          |                                         +------------------+ |
 *          +--------------------------------------------------------------+
 */

/**
 * Builds fan-out shape stream out of arbitrary typed Source and Sink.
 * Fan-out shape is build throw Sink cloning and type conversions between Source and Sink.
 * @param numOuts number of cloned sinks
 * @tparam Out source data type
 * @tparam In sink data type
 * @tparam SrcMat source materialized value type
 * @tparam SnkMat sink materialized value type
 */

abstract class ClonedSinkFanOutStream[Out, In, SrcMat, SnkMat](numOuts: Int) {

  assert(numOuts > 0, s"$numOuts must be greater than 0")

  /**
   * Stream source.
   */
  def source: Source[Out, SrcMat]

  /**
   * Stream sink(s).
   */
  def sink: Sink[In, SnkMat]

  /**
   * Data type converter between source and sink.
   */
  def typeConverter: Out => In

  /**
   * Clones and converts sink values type (In) to source type (Out).
   */
  def sinks: IndexedSeq[Sink[Out, (Int, SnkMat)]] = (1 to numOuts).
    map{ num =>
      Flow[Out].
        map(typeConverter).
        toMat(sink)(Keep.right).
        mapMaterializedValue(value => (num, value))
    }

  /**
   * Results in stream as [[RunnableGraph]]. Stream materialized value combined as sequence
   * of sink id (an integer from 1 to numOuts) and original sink materialized value (of type SnkMat).
   */
  def stream: RunnableGraph[Seq[(Int, SnkMat)]] =
    RunnableGraph.fromGraph( GraphDSL.create(sinks) { implicit builder => clonedSinks =>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val balancer = builder.add(Balance[Out](clonedSinks.size))
      source ~> balancer
      clonedSinks.foreach(sink => balancer ~> sink)
      ClosedShape
    }).named("ClonedSinkFanOutStream")

}