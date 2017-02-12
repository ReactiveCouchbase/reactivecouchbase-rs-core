package org.reactivecouchbase.scaladsl

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import org.reactivestreams.Publisher

import scala.concurrent.Future

sealed trait QueryLike

case class N1qlQuery(n1ql: String, params: Map[String, String] = Map.empty) extends QueryLike

// TODO : work on naming
trait QueryResult[T] {
  def asSource: Source[T, _]
  def map[U](f: T => U): QueryResult[U] = SimpleQueryResult(() => asSource.map(f))
  def flatMap[U, M](f: T => Source[U, M]): QueryResult[U] = SimpleQueryResult(() => asSource.flatMapConcat(f))
  def asPublisher(fanout: Boolean = true)(implicit materializer: Materializer): Publisher[T] = asSource.runWith(Sink.asPublisher(fanout))(materializer)
  def fold[U](zero: U)(reducer: (U, T) => U)(implicit materializer: Materializer): Future[U] = asSource.runFold(zero)(reducer)(materializer)
  def foldAsync[U](zero: U)(reducer: (U, T) => Future[U])(implicit materializer: Materializer): Future[U] = asSource.runFoldAsync(zero)(reducer)(materializer)
  def asSeq(implicit materializer: Materializer): Future[Seq[T]] = asSource.runWith(Sink.seq[T])(materializer)
  def one(implicit materializer: Materializer): Future[Option[T]] = asSource.runWith(Sink.headOption)(materializer)
}

private[scaladsl] case class SimpleQueryResult[T](source: () => Source[T, _]) extends QueryResult[T] {
  lazy val lazySource = source()
  override def asSource: Source[T, _] = lazySource
}