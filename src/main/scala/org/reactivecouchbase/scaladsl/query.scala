package org.reactivecouchbase.scaladsl

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import org.reactivestreams.Publisher
import play.api.libs.json._
import com.couchbase.client.java.view.{SpatialViewQuery => CouchbaseSpatialViewQuery}
import com.couchbase.client.java.view.{ViewQuery => CouchbaseViewQuery}
import com.couchbase.client.java.search.{SearchQuery => CouchbaseSearchQuery}

import scala.concurrent.{ExecutionContext, Future}

sealed trait QueryLike

case class N1qlQuery(n1ql: String, params: JsObject = Json.obj()) extends QueryLike {
  def on(args: JsObject) = copy(params = args)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

case class SpatialQuery(query: CouchbaseSpatialViewQuery) extends QueryLike

case class SpatialViewRow[T](id: String, key: JsValue, value: JsValue, geometry: JsValue, doc: Future[JsValue], reader: Reads[T]) {
  def typed(implicit ec: ExecutionContext): Future[T] = doc.map(j => reader.reads(j).get)
}

object SpatialQuery {
  def apply(designDoc: String, view: String, f: CouchbaseSpatialViewQuery => CouchbaseSpatialViewQuery = identity): SpatialQuery = {
    SpatialQuery(CouchbaseSpatialViewQuery.from(designDoc, view))
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

case class ViewRow[T](id: String, key: JsValue, value: JsValue, doc: Future[JsValue], reader: Reads[T]) {
  def typed(implicit ec: ExecutionContext): Future[T] = doc.map(j => reader.reads(j).get)
}

case class ViewQuery(query: CouchbaseViewQuery) extends QueryLike

object ViewQuery {
  def apply(designDoc: String, view: String, f: CouchbaseViewQuery => CouchbaseViewQuery = identity): ViewQuery = {
    ViewQuery(CouchbaseViewQuery.from(designDoc, view))
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO : work on naming
trait QueryResult[T] {
  def asSource: Source[T, _]
  def asSeq(implicit materializer: Materializer): Future[Seq[T]] = asSource.runWith(Sink.seq[T])(materializer)
  def asPublisher(fanout: Boolean = true)(implicit materializer: Materializer): Publisher[T] = asSource.runWith(Sink.asPublisher(fanout))(materializer)
  def asHeadOption(implicit materializer: Materializer): Future[Option[T]] = asSource.runWith(Sink.headOption)(materializer)
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def map[U](f: T => U): QueryResult[U] = SimpleQueryResult(() => asSource.map(f))
  def flatMap[U, M](f: T => Source[U, M]): QueryResult[U] = SimpleQueryResult(() => asSource.flatMapConcat(f))
  def fold[U](zero: U)(reducer: (U, T) => U)(implicit materializer: Materializer): Future[U] = asSource.runFold(zero)(reducer)(materializer)
  def foldAsync[U](zero: U)(reducer: (U, T) => Future[U])(implicit materializer: Materializer): Future[U] = asSource.runFoldAsync(zero)(reducer)(materializer)
}

private[scaladsl] case class SimpleQueryResult[T](source: () => Source[T, _]) extends QueryResult[T] {
  lazy val lazySource = source()
  override def asSource: Source[T, _] = lazySource
}