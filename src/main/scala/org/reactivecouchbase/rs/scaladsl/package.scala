package org.reactivecouchbase.rs

import akka.stream.Materializer
import rx.Observable

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NoStackTrace

package object scaladsl {

  type ExecCtx[_] = ExecutionContext
  type Mat[_] = Materializer

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
  }
}
