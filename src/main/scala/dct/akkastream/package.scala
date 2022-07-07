package dct

import akka.actor.ActorSystem
import akka.stream.alpakka.slick.scaladsl.SlickSession

import scala.concurrent.ExecutionContext

package object akkastream {
  val SINK_PARALLELISM: Int = 4
  implicit val system: ActorSystem = ActorSystem("Data-Copy-Tool")
  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("blocking-io-dispatcher")
}
