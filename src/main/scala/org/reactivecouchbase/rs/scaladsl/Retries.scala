package org.reactivecouchbase.rs.scaladsl

/**
  * Handles the retries on error. Similar to `RetryBuilder` of java client of couchbase.
  */
object Retries {

  import scala.concurrent._
  import scala.concurrent.duration._
  import akka.pattern.after
  import akka.actor.Scheduler

  /**
    * Tries to invoke a future block as many time as asked on occurrence of a specific type of error. A fix delay/interval
    * is applied before each invocation.
    * Note that if the errors keep occurring more than the maximum allowed number of attempts, the last error that
    * triggered the extraneous attempt will be wrapped as the cause inside a CannotRetryException
    *
    * @param retries   Number of retries to be made
    * @param delay     Interval between two consecutive retries
    * @param errFilter Function to filter a specific type of error
    * @param f         Block of code to be retried, so this should be the couchbase query to retry on failure
    * @param ec        Execution context
    * @param s         Scheduler
    * @tparam T Type of result
    * @return Either the result inside a future or an exception
    */
  def retryOnError[T](
                       retries: Int = 1,
                       delay: FiniteDuration = 0.millis,
                       errFilter: Throwable => Boolean = _ => true
                     )(
                       f: => Future[T]
                     )(
                       implicit ec: ExecutionContext,
                       s: Scheduler
                     ): Future[T] = {
    f recoverWith {
      case e if errFilter(e) && retries > 0 =>
        after(delay, s)(retryOnError(retries - 1, delay)(f))

      case e if errFilter(e) && retries == 0 =>
        Future.failed(CannotRetryException(s"All retries failed because of ${e.getMessage}"))
    }

  }
}


/**
  * Thrown exception if error persist even after maximum number of retries.
  *
  * @param message Message exception
  */
case class CannotRetryException(message: String) extends RuntimeException(message)