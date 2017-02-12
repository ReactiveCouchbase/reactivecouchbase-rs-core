package org.reactivecouchbase.scaladsl

import rx.functions.Func1

object RxUtils {
  def func1[T, R](f: T => R): Func1[T, R] = new Func1[T, R]() {
    override def call(t: T): R = f(t)
  }
}