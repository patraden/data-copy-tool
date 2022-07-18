package dct

import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext

package object akkastreams {
  /**
   * Shared akka actor system to run all streams in dct.
   */
  implicit val system: ActorSystem = ActorSystem("Data-Copy-Tool")
  /**
   * Dedicated blocking behaviour dispatcher configurable throw application.conf
   * See also [[https://doc.akka.io/docs/akka/current/typed/dispatchers.html#blocking-needs-careful-management Blocking needs]]
   */
  implicit val executionContext: ExecutionContext = system.dispatchers.lookup("dct.akka.blocking-io-dispatcher")
}
