package org.reactivecouchbase.rs.scaladsl

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import org.reactivestreams.Publisher
import com.couchbase.client.java.view.{SpatialViewQuery => CouchbaseSpatialViewQuery}
import com.couchbase.client.java.view.{ViewQuery => CouchbaseViewQuery}
import org.reactivecouchbase.rs.scaladsl.json.JsonReads
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.concurrent.{ExecutionContext, Future}

sealed trait QueryLike
sealed trait ViewQueryLike

case class N1qlQuery(n1ql: String, params: JsObject = Json.obj()) extends QueryLike {
  def on(args: JsObject) = copy(params = args)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

case class SpatialQuery(query: CouchbaseSpatialViewQuery) extends ViewQueryLike

case class SpatialViewRow[T](id: String, key: JsValue, value: JsValue, geometry: JsValue, doc: Future[ByteString], reader: JsonReads[T]) {
  def typed(implicit ec: ExecutionContext): Future[T] = doc.map(j => reader.reads(j).get)
}

object SpatialQuery {
  def apply(designDoc: String, view: String, f: CouchbaseSpatialViewQuery => CouchbaseSpatialViewQuery = identity): SpatialQuery = {
    SpatialQuery(f(CouchbaseSpatialViewQuery.from(designDoc, view)))
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

case class ViewRow[T](id: String, key: JsValue, value: JsValue, doc: Future[ByteString], reader: JsonReads[T]) {
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
  def asSource: Source[T, Mat]
  def asSeq(implicit materializer: Materializer): Future[Seq[T]] = asSource.runWith(Sink.seq[T])(materializer)
  def asPublisher(fanout: Boolean = true)(implicit materializer: Materializer): Publisher[T] = asSource.runWith(Sink.asPublisher(fanout))(materializer)
  def asHeadOption(implicit materializer: Materializer): Future[Option[T]] = asSource.runWith(Sink.headOption)(materializer)
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def map[U](f: T => U): QueryResult[U, Mat] = SimpleQueryResult(() => asSource.map(f))
  def flatMap[U, M](f: T => Source[U, M]): QueryResult[U, Mat] = SimpleQueryResult(() => asSource.flatMapConcat(f))
  def fold[U](zero: U)(reducer: (U, T) => U)(implicit materializer: Materializer): Future[U] = asSource.runFold(zero)(reducer)(materializer)
  def foldAsync[U](zero: U)(reducer: (U, T) => Future[U])(implicit materializer: Materializer): Future[U] = asSource.runFoldAsync(zero)(reducer)(materializer)
}

private[scaladsl] case class SimpleQueryResult[T, Mat](source: () => Source[T, Mat]) extends QueryResult[T, Mat] {
  private lazy val lazySource: Source[T, Mat] = source()
  override def asSource: Source[T, Mat] = lazySource
}