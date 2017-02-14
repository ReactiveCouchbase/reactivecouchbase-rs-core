package org.reactivecouchbase.scaladsl

import rx.Observable

import scala.concurrent.{Future, Promise}
import scala.util.control.NoStackTrace

case object ObservableCompletedWithoutValue extends RuntimeException("Observable should have produced at least a value ...") with NoStackTrace

object Implicits {

  implicit class EnhancedObservable[T](obs: Observable[T]) {
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
