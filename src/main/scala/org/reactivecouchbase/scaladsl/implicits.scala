package org.reactivecouchbase.scaladsl

import rx.Observable
import rx.functions.{Action0, Action1}

import scala.concurrent.{Future, Promise}

object Implicits {

  implicit class EnhancedObservable[T](obs: Observable[T]) {
    def asFuture: Future[T] = {
      val p = Promise[T]
      obs.subscribe(
        new Action1[T] {
          override def call(t: T): Unit = p.trySuccess(t)
        },
        new Action1[Throwable] {
          override def call(t: Throwable): Unit = p.tryFailure(t)
        },
        new Action0 {
          override def call(): Unit = {
            if (!p.isCompleted) {
              p.tryFailure(new RuntimeException("Observable should have produced at least a value ..."))
            }
          }
        }
      )
      p.future
    }
  }
}
