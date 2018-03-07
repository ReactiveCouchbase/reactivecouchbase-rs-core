package org.reactivecouchbase.rs.scaladsl

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.couchbase.client.java.document.RawJsonDocument
import com.couchbase.client.java.view.{
  AsyncSpatialViewRow,
  AsyncViewRow,
  SpatialViewQuery => CouchbaseSpatialViewQuery,
  ViewQuery => CouchbaseViewQuery
}
import org.reactivecouchbase.rs.scaladsl.json.{CouchbaseJsonDocConverter, EmptyQueryParam, JsonReads, QueryParams}
import org.reactivestreams.Publisher

import scala.concurrent.{ExecutionContext, Future}

sealed trait QueryLike
sealed trait ViewQueryLike

case class N1qlQuery(query: String, params: QueryParams) extends QueryLike {
  def on(args: QueryParams) = copy(params = args)
}

object N1qlQuery {
  def apply(query: String): N1qlQuery = N1qlQuery(query, EmptyQueryParam)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

case class SpatialQuery(query: CouchbaseSpatialViewQuery) extends ViewQueryLike

case class SpatialViewRow[T](underlying: AsyncSpatialViewRow, reader: JsonReads[T]) {
  def id: String                                                 = underlying.id()
  def key[A](implicit converter: CouchbaseJsonDocConverter[A])   = converter.convertTo(underlying.key())
  def value[A](implicit converter: CouchbaseJsonDocConverter[A]) = converter.convertTo(underlying.value())
  def doc(implicit ec: ExecutionContext): Future[ByteString] =
    underlying.document(classOf[RawJsonDocument]).asFuture.filter(_ != null).map(a => ByteString(a.content()))
  def typed(implicit ec: ExecutionContext): Future[T] = doc.map(j => reader.reads(j).get)
}

object SpatialQuery {
  def apply(designDoc: String,
            view: String,
            f: CouchbaseSpatialViewQuery => CouchbaseSpatialViewQuery = identity): SpatialQuery = {
    SpatialQuery(f(CouchbaseSpatialViewQuery.from(designDoc, view)))
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

case class ViewRow[T](underlying: AsyncViewRow, reader: JsonReads[T]) {
  def id: String                                                 = underlying.id()
  def key[A](implicit converter: CouchbaseJsonDocConverter[A])   = converter.convertTo(underlying.key())
  def value[A](implicit converter: CouchbaseJsonDocConverter[A]) = converter.convertTo(underlying.value())
  def doc(implicit ec: ExecutionContext): Future[ByteString] =
    underlying.document(classOf[RawJsonDocument]).asFuture.filter(_ != null).map(a => ByteString(a.content()))
  def typed(implicit ec: ExecutionContext): Future[T] = doc.map(j => reader.reads(j).get)
}

case class ViewQuery(query: CouchbaseViewQuery) extends ViewQueryLike

object ViewQuery {
  def apply(designDoc: String, view: String, f: CouchbaseViewQuery => CouchbaseViewQuery = identity): ViewQuery = {
    ViewQuery(f(CouchbaseViewQuery.from(designDoc, view)))
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

trait QueryResult[T, Mat] {
  // conversions
  def asSource: Source[T, Mat]
  def asSeq(implicit materializer: Materializer): Future[Seq[T]] = asSource.runWith(Sink.seq[T])(materializer)
  def asPublisher(fanout: Boolean = true)(implicit materializer: Materializer): Publisher[T] =
    asSource.runWith(Sink.asPublisher(fanout))(materializer)
  def asHeadOption(implicit materializer: Materializer): Future[Option[T]] =
    asSource.runWith(Sink.headOption)(materializer)
  // transformations
  def map[U](f: T => U): QueryResult[U, Mat]                   = SimpleQueryResult(() => asSource.map(f))
  def flatMap[U, M](f: T => Source[U, M]): QueryResult[U, Mat] = SimpleQueryResult(() => asSource.flatMapConcat(f))
  def fold[U](zero: U)(reducer: (U, T) => U)(implicit materializer: Materializer): Future[U] =
    asSource.runFold(zero)(reducer)(materializer)
  def foldAsync[U](zero: U)(reducer: (U, T) => Future[U])(implicit materializer: Materializer): Future[U] =
    asSource.runFoldAsync(zero)(reducer)(materializer)
}

private[scaladsl] case class SimpleQueryResult[T, M](source: () => Source[T, M]) extends QueryResult[T, M] {
  private lazy val lazySource: Source[T, M] = source()
  override def asSource: Source[T, M]       = lazySource
}
