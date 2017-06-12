package org.reactivecouchbase.rs.scaladsl.json

import akka.util.ByteString

case class JsonValidationError(messages: Seq[String]) {
  lazy val message = messages.last
}

trait JsonReads[A] { self =>
  def reads(json: ByteString): JsonResult[A]
}

object JsonReads {
  def apply[A](f: ByteString => JsonResult[A]): JsonReads[A] = (json: ByteString) => f(json)
}

trait JsonWrites[-A] { self =>
  def writes(o: A): ByteString
}

object JsonWrites {
  def apply[A](f: A => ByteString): JsonWrites[A] = (a: A) => f(a)
}

trait JsonFormat[A] extends JsonWrites[A] with JsonReads[A]

object JsonFormat {
  def apply[A](fjs: JsonReads[A], tjs: JsonWrites[A]): JsonFormat[A] = new JsonFormat[A] {
    def reads(json: ByteString) = fjs.reads(json)
    def writes(o: A) = tjs.writes(o)
  }
}

sealed trait JsonResult[+A] { self =>

  def isSuccess: Boolean

  def isError: Boolean
  
  def map[B](f: A => B): JsonResult[B]
  
  def flatMap[B](f: A => JsonResult[B]): JsonResult[B]
  
  def foreach(f: A => Unit): Unit

  def filterNot(error: JsonError)(p: A => Boolean): JsonResult[A] =
    flatMap { a => if (p(a)) error else JsonSuccess(a) }

  def filterNot(p: A => Boolean): JsonResult[A] =
    flatMap { a => if (p(a)) JsonError() else JsonSuccess(a) }

  def filter(p: A => Boolean): JsonResult[A] =
    flatMap { a => if (p(a)) JsonSuccess(a) else JsonError() }

  def filter(otherwise: JsonError)(p: A => Boolean): JsonResult[A] =
    flatMap { a => if (p(a)) JsonSuccess(a) else otherwise }

  def collect[B](otherwise: JsonValidationError)(p: PartialFunction[A, B]): JsonResult[B] = flatMap {
    case t if p.isDefinedAt(t) => JsonSuccess(p(t))
    case _ => JsonError(Seq(otherwise))
  }

  def get: A

  def getOrElse[AA >: A](t: => AA): AA

  def orElse[AA >: A](t: => JsonResult[AA]): JsonResult[AA]
  
  def asOpt: Option[A]

  def asEither: Either[Seq[JsonValidationError], A]
  
  def recover[AA >: A](errManager: PartialFunction[JsonError, AA]): JsonResult[AA]

  def recoverTotal[AA >: A](errManager: JsonError => AA): AA
}

case class JsonSuccess[T](value: T) extends JsonResult[T] {

  override def isSuccess: Boolean = true

  override def isError: Boolean = false

  override def map[B](f: (T) => B): JsonResult[B] = JsonSuccess(f(value))

  override def flatMap[B](f: (T) => JsonResult[B]): JsonResult[B] = f(value)

  override def foreach(f: (T) => Unit): Unit = f(value)

  override def get: T = value

  override def getOrElse[AA >: T](t: => AA): AA = value

  override def orElse[AA >: T](t: => JsonResult[AA]): JsonResult[AA] = JsonSuccess(value)

  override def asOpt: Option[T] = Some(value)

  override def asEither: Either[Seq[JsonValidationError], T] = Right(value)

  override def recover[AA >: T](errManager: PartialFunction[JsonError, AA]): JsonResult[AA] = JsonSuccess(value)

  override def recoverTotal[AA >: T](errManager: (JsonError) => AA): AA = value
}

case class JsonError(errors: Seq[JsonValidationError] = Seq.empty[JsonValidationError]) extends JsonResult[Nothing] {

  override def isSuccess: Boolean = false

  override def isError: Boolean = true

  override def map[B](f: (Nothing) => B): JsonResult[B] = JsonError(errors)

  override def flatMap[B](f: (Nothing) => JsonResult[B]): JsonResult[B] = JsonError(errors)

  override def foreach(f: (Nothing) => Unit): Unit = ()

  override def get: Nothing = throw new NoSuchElementException("JsonError.get")

  override def getOrElse[AA >: Nothing](t: => AA): AA = t

  override def orElse[AA >: Nothing](t: => JsonResult[AA]): JsonResult[AA] = t

  override def asOpt: Option[Nothing] = None

  override def asEither: Either[Seq[JsonValidationError], Nothing] = Left(errors)

  override def recover[AA >: Nothing](errManager: PartialFunction[JsonError, AA]): JsonResult[AA] = if (errManager.isDefinedAt(this)) {
    JsonSuccess(errManager.apply(this))
  } else {
    JsonError(errors)
  }

  override def recoverTotal[AA >: Nothing](errManager: (JsonError) => AA): AA = errManager(this)
}