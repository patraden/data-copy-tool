
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Future}

package object dct {

  /**
   * Extended [[Future]] implicit with "transactional" recovery and verbose logging.
   * @see [[https://stackoverflow.com/questions/60952195/compose-futures-with-recovery-in-scala/60955010#60955010 SO answer]]
   */
  class TransactFuture[T](underlying: Future[T], rollback: PartialFunction[Throwable, Future[Unit]], val sys: ActorSystem)
    extends dct.spark.Logger {

    def flatMap[S](f: T => Future[S])(implicit executor: ExecutionContext): Future[S] = {
      underlying.flatMap(f).recoverWith {
        case exception: Throwable =>
          val failure: Future[S] = Future.failed[S](exception)
          rollback.lift(exception).
            fold{
              Future.
                successful(logInfo(s"No recovery available for ${exception.getClass.getName}")).
                flatMap(_ =>
                  Future.
                    successful(logInfo("Forced termination of the akka Actor system")).
                    flatMap(_ => sys.terminate()).
                    flatMap(_ => failure))
            } { rb =>
              Future.successful(logInfo(s"Applying recovery from ${exception.getClass.getName}")).
                flatMap(_ =>
                  rb.
                    map(_ => logInfo(s"Applied recovery for ${exception.getClass.getName}")).
                    flatMap(_ => failure)
                )
            }
      }
    }
  }

  /**
   * [[Future]] implicit extension to [[TransactFuture]].
   */
  implicit class FutureOps[T](underling: Future[T]) {
    def rollbackWith(rollback: PartialFunction[Throwable, Future[Unit]], sys: ActorSystem): TransactFuture[T] = {
      new TransactFuture[T](underling, rollback, sys)
    }
  }

}