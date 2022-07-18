package dct.akkastreams

import org.scalatest.funsuite.AsyncFunSuite
import scala.concurrent.Future
import akka.stream.scaladsl.{Sink, Source}
import akka.{Done, NotUsed}

class ClonedSinkFanOutStreamTestSuit extends AsyncFunSuite {

  test("ClonedSinkFanOutStream wrong inputs") {

    class TestStream
      extends ClonedSinkFanOutStream[Int, String, NotUsed, Future[Done]](0) {
      def source: Source[Int, NotUsed] = Source.empty[Int]
      def sink: Sink[String, Future[Done]] = Sink.ignore
      override def typeConverter: Int => String = (i: Int) => i.toString
    }

    Future(assertThrows[AssertionError](new TestStream))
  }

  test("ClonedSinkFanOutStream balancing") {

    class TestStream(numOuts: Int)
      extends ClonedSinkFanOutStream[Int, String, NotUsed, Future[List[String]]](numOuts) {
      def source: Source[Int, NotUsed] = Source(1 to 1000002)
      def sink: Sink[String, Future[List[String]]] = Sink.collection[String, List[String]]
      def typeConverter: Int => String = (i: Int) => i.toString
    }

    Future.sequence(
      new TestStream(5).stream.run().
        map{ case (sinkId, values) => values.map(value => (sinkId, value.length))}
    ).map(seq =>
      assert(
        seq.toMap == Map(
          1 -> 200001,
          2 -> 200001,
          3 -> 200000,
          4 -> 200000,
          5 -> 200000
        )
      )
    )
  }


}
