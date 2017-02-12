package org.reactivecouchbase.scaladsl

import rx.functions.{Action0, Action1, Func1}

object RxUtils {
  def func1[T, R](f: T => R): Func1[T, R] = new Func1[T, R]() {
    override def call(t: T): R = f(t)
  }
  def action1[T](f: T => _): Action1[T] = new Action1[T] {
    override def call(t: T): Unit = f(t)
  }
  def action0[T](f: () => _): Action0 = new Action0 {
    override def call(): Unit = f()
  }
}