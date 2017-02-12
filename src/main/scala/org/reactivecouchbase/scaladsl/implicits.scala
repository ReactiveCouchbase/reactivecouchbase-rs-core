package org.reactivecouchbase.scaladsl

import rx.Observable

import scala.concurrent.{Future, Promise}

case class ObservableCompletedWithoutValue(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object Implicits {

  implicit class EnhancedObservable[T](obs: Observable[T]) {
    def asFuture: Future[T] = {
      val p = Promise[T]
      obs.subscribe(
        RxUtils.action1(p.trySuccess(_)),
        RxUtils.action1(p.tryFailure(_)),
        RxUtils.action0 { () =>
          if (!p.isCompleted) {
            p.tryFailure(new ObservableCompletedWithoutValue("Observable should have produced at least a value ..."))
          }
        }
      )
      p.future
    }
  }
}
