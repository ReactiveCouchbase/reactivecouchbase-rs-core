package org.reactivecouchbase.scaladsl

import rx.Observable
import rx.functions.Action1

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

object Implicits {

  implicit class EnhancedObject(obj: Any) {
    def debug: Any = {
      println(if (obj != null) obj.toString else "null")
      obj
    }

    def debug(label: String): Any = {
      println(if (obj != null) s"$label: ${obj.toString}" else s"$label: null")
      obj
    }
  }

  implicit class EnhancedFuture[T](fu: Future[T]) {
    def await: T = Await.result(fu, Duration.Inf)
  }

  implicit class EnhancedObservable[T](obs: Observable[T]) {
    def asFuture: Future[T] = {
      val p = Promise[T]
      obs.doOnNext(new Action1[T] {
        override def call(t: T): Unit = p.trySuccess(t)
      })
      obs.doOnError(new Action1[Throwable] {
        override def call(t: Throwable): Unit = p.tryFailure(t)
      })
      p.future
    }
  }
}
