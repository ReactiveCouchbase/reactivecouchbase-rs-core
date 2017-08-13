package org.reactivecouchbase.rs

import akka.stream.Materializer
import rx.Observable

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NoStackTrace
import scala.concurrent.duration._

package object scaladsl {

  type ExecCtx[_] = ExecutionContext
  type Mat[_] = Materializer

  val InfiniteExpiry: Duration = Duration.Inf

  case object ObservableCompletedWithoutValue extends RuntimeException("Observable should have produced at least a value ...") with NoStackTrace

  implicit class EnhancedObservable[T](val obs: Observable[T]) extends AnyVal {
    def asFuture: Future[T] = {
      val p = Promise[T]
      obs.subscribe(
        RxUtils.action1(p.trySuccess(_)),
        RxUtils.action1(p.tryFailure(_)),
        RxUtils.action0 { () =>
          if (!p.isCompleted) {
            p.tryFailure(ObservableCompletedWithoutValue)
          }
        }
      )
      p.future
    }

    /**
      * Applies a timeout given Some[Duration], otherwise does nothing
      *
      * @param timeout: Optional [[scala.concurrent.duration.Duration]]
      *
      * @return obs Observable[T]
      * */
    def maybeTimeout(timeout: Option[Duration]): Observable[T] =
      timeout.map(to => obs.timeout(to.length, to.unit)).getOrElse(obs)
  }

  implicit class EnhancedDuration(val duration: Duration) extends AnyVal {
    def asCouchbaseExpiry: Int = {
      duration match {
        case Duration.Zero => 0
        case Duration.Inf => 0
        case _ => 
          val start = if (duration < 30.days) 0L else System.currentTimeMillis / 1000L
          (start + duration.toSeconds).toInt
      }
    }
  }
}
