package dct

import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext

package object akkastream {
  /**
   * Shared akka actor system to run all streams in dct.
   */
  implicit val system: ActorSystem = ActorSystem("Data-Copy-Tool")
  /**
   * Dedicated blocking behaviour dispatcher configurable throw application.conf
   * See also [[https://doc.akka.io/docs/akka/current/typed/dispatchers.html#blocking-needs-careful-management Blocking needs]]
   */
  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("blocking-io-dispatcher")
  /**
   * Defines number of outlets for sink fan-out Flow.
   * This is a helpful configuration to use full sink throughput capacity.
   */
  val SINK_PARALLELISM: Int = 4
}
